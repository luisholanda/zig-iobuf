/// A chain of IOBufs.
///
/// A chain allows to represent a single logically contiguous data as a multiple non-contiguous
/// chunks of data. This is useful when we can't receive the total data in a single buffer, e.g.
/// a HTTP request body, or a file. It is also useful when queueing chunks of data to be sent
/// over the network.
const std = @import("std");

const IOBuf = @import("IOBuf.zig");
const Self = @This();

const Node = struct {
    buf: IOBuf = .{},
    prev: *IOBuf = undefined,
    next: *IOBuf = undefined,

    inline fn fromBufPtr(buf: *IOBuf) *Node {
        return @fieldParentPtr("buf", buf);
    }
};

/// The first node in the chain.
first: *IOBuf,
/// Allocator used for chain nodes.
allocator: std.mem.Allocator,
/// Allocator used for buffer data.
buf_allocator: std.mem.Allocator,

/// Initializes a new chain with a given amount of total capacity, but keeping each buffer with a max capacity.
pub fn init(total_capacity: usize, max_buf_capacity: usize, allocator: std.mem.Allocator, buf_allocator: std.mem.Allocator) !Self {
    var chain = try initSingle(@min(total_capacity, max_buf_capacity), allocator, buf_allocator);
    errdefer chain.deinit();

    var remaining_capacity = total_capacity - chain.first.capacity;
    while (remaining_capacity > 0) {
        const node = try chain.appendNode();
        try node.buf.init(@min(remaining_capacity, max_buf_capacity), buf_allocator);
        remaining_capacity -= node.buf.capacity;
    }

    return chain;
}

/// Initializes a chain with a single node.
pub fn initSingle(capacity: usize, allocator: std.mem.Allocator, buf_allocator: std.mem.Allocator) !Self {
    const node = try allocator.create(Node);
    errdefer allocator.destroy(node);
    try node.buf.init(capacity, buf_allocator);

    node.prev = &node.buf;
    node.next = node.prev;

    return .{
        .first = &node.buf,
        .allocator = allocator,
        .buf_allocator = buf_allocator,
    };
}

/// Deinitializes the entire chain.
pub fn deinit(self: Self) void {
    var curr = self.first;

    while (true) {
        curr.deinit();
        const node = Node.fromBufPtr(curr);
        curr = node.next;

        self.allocator.destroy(node);

        if (curr == self.first) break;
    }
}

/// Total length of the chain.
pub fn chainLength(self: Self) usize {
    var len: usize = 0;
    var it = self.iterator();
    while (it.next()) |buf| len += buf.len;

    return len;
}

/// The number of buffers in the chain.
pub fn bufCount(self: Self) usize {
    var count: usize = 0;
    var it = self.iterator();
    while (it.next()) |_| : (count += 1) {}
    return count;
}

/// Clones the entinre chain into a new chain.
///
/// Both chains shares the buffers and allocators.
pub fn clone(self: Self) !Self {
    const first = try self.allocator.create(Node);
    first.buf = self.first.clone();
    first.prev = &first.buf;
    first.next = &first.buf;

    var cloned: Self = .{
        .first = &first.buf,
        .allocator = self.allocator,
        .buf_allocator = self.buf_allocator,
    };
    errdefer cloned.deinit();

    var it = self.iterator();
    _ = it.next(); // ignore first, already taken care of.
    while (it.next()) |buf| {
        try cloned.append(buf.clone());
    }

    return cloned;
}

/// Ensures no buffer is shared with any other IOBuf.
///
/// If there is a shared buffer in the chain, the entire chain will be coalesced.
pub fn unshare(self: *Self) !void {
    var it = self.iterator();
    while (it.next()) |buf| {
        if (buf.isShared()) break;
    } else return;

    try self.coalesce();
}

/// Coalesces the chain to a single node.
///
/// After this returns, the chain shares no data with any other IOBuf.
///
/// If this fails, no change is made to the chain.
pub fn coalesce(self: *Self) !void {
    if (self.first == Node.fromBufPtr(self.first).prev)
        return self.first.unshare(self.buf_allocator);

    var headroom = self.first.headroom();
    const tailroom = Node.fromBufPtr(self.first).prev.tailroom();
    var new_len = self.chainLength() +| tailroom;

    // As in IOBuf.reserveSlow, we assume that tailroom is more important than headroom.
    // Thus if including the headroom would cause an overflow, just ignore it.
    new_len = std.math.add(usize, new_len, headroom) catch b: {
        headroom = 0;
        break :b new_len;
    };

    const new = try Self.initSingle(new_len, self.allocator, self.buf_allocator);

    new.first.trimStart(headroom);

    var it = self.iterator();
    while (it.next()) |buf| {
        if (buf.len > 0)
            new.first.appendSlice(buf.data());
    }

    self.deinit();
    self.first = new.first;
}

const Iterator = struct {
    curr_node: *IOBuf,
    end_node: *IOBuf,
    started: bool = false,

    pub fn next(self: *Iterator) ?*IOBuf {
        if (self.curr_node == self.end_node and self.started) return null;
        self.started = true;

        const curr = self.curr_node;
        self.curr_node = Node.fromBufPtr(self.curr_node).next;

        return curr;
    }
};

/// An iterator over the buffers in the chain.
pub inline fn iterator(self: *const Self) Iterator {
    return .{
        .curr_node = self.first,
        .end_node = self.first,
    };
}

/// Append an IOBuf to the chain.
///
/// Iterators aren't invalidated by this call.
pub fn append(self: *Self, buf: IOBuf) !void {
    const node = try self.appendNode();
    node.buf = buf;
}

/// Append a chain to this chain.
///
/// If they share the same allocator, this function is guaranteed to succeed. If not,
/// it can fail while allocating new nodes in our chain. In this case, the function
/// guarantees that the chains will be kept intact and is responsibility of the caller
/// to clean them up if necessary.
///
/// If the function succeeds, the ownership of `other` is moved to it.
pub fn appendChain(self: *Self, other: Self) !void {
    // Both chains share the same allocator, we can just swap pointers.
    if (self.allocator.ptr == other.allocator.ptr) {
        const self_head = Node.fromBufPtr(self.first);
        const other_head = Node.fromBufPtr(other.first);

        const self_last = Node.fromBufPtr(self_head.prev);
        const other_last = Node.fromBufPtr(other_head.prev);

        other_head.prev = &self_last.buf;
        self_last.next = other.first;

        self_head.prev = &other_last.buf;
        other_last.next = self.first;
    } else {
        // The chains use different allocators, we need to copy the nodes, as there is no guarantee
        // that our allocator will be able to free the other nodes.
        const old_prev = Node.fromBufPtr(self.first).prev;
        errdefer self.splitAfter(old_prev).deinit();

        var it = other.iterator();
        while (it.next()) |buf| {
            try self.append(buf.clone());
        }

        other.deinit();
    }
}

/// Removes all the nodes in the chain after `buf`, returning them in a new chain.
///
/// E.g., if the chain is (A -> B -> C -> D -> E), `A.splitAfter(C)` return (D -> E)
/// and the chain is now (A -> B -> C).
pub fn splitAfter(self: Self, buf: *IOBuf) Self {
    const buf_node = Node.fromBufPtr(buf);

    const split_head = buf_node.next;
    const split_tail = Node.fromBufPtr(self.first).prev;

    Node.fromBufPtr(self.first).prev = buf;
    buf_node.next = self.first;

    Node.fromBufPtr(split_head).prev = split_tail;
    Node.fromBufPtr(split_tail).next = split_head;

    return .{
        .first = split_head,
        .allocator = self.allocator,
        .buf_allocator = self.buf_allocator,
    };
}

/// Removes all the nodes in a range from the chain, returning them in a new chain.
///
/// E.g. if the chain is (A -> B -> C -> D -> E), `A.removeRange(B, D)` return (B -> C -> D)
/// and the chain is now (A -> E).
pub fn removeRange(self: *Self, start: *IOBuf, end: *IOBuf) Self {
    const start_node = Node.fromBufPtr(start);
    const end_node = Node.fromBufPtr(end);

    Node.fromBufPtr(start_node.prev).next = end_node.next;
    Node.fromBufPtr(end_node.next).prev = start_node.prev;

    start_node.prev = end;
    end_node.next = start;

    return .{
        .first = start,
        .allocator = self.allocator,
        .buf_allocator = self.buf_allocator,
    };
}

/// Creates a slice of iovecs for vectored writes.
pub fn iovecs(self: *Self) !std.ArrayList(std.posix.iovec) {
    const count = self.bufCount();
    var iovecs_ = try std.ArrayList(std.posix.iovec).initCapacity(self.allocator, count);
    errdefer iovecs_.deinit();

    var i: usize = 0;
    var it = self.iterator();
    while (it.next()) |buf| : (i += 1) {
        try buf.unshare(self.allocator);
        // SAFETY: we already allocated enough memory.
        iovecs_.append(.{ .base = buf.writableData().ptr, .len = buf.len }) catch unreachable;
    }

    return iovecs_;
}

fn appendNode(self: *Self) !*Node {
    const node = try self.allocator.create(Node);
    node.buf = .{};

    node.next = self.first;
    node.prev = Node.fromBufPtr(self.first).prev;
    Node.fromBufPtr(self.first).prev = &node.buf;
    Node.fromBufPtr(node.prev).next = &node.buf;

    return node;
}
