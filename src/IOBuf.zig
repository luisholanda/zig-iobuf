/// IOBuf allows for safe zero-copy access to a bytes buffer.
///
/// # Initialization
///
/// A default created IOBuf is empty and unmanaged. Every IOBuf must be
/// initialized either with one of the `init*` functions or one of the
/// `dupe*` functions.
///
/// # Unmanaged Buffers
///
/// In some cases, it may be useful to explicitly manage the buffer used
/// by the IOBuf. This can be done with the `initUnmanaged` initializer
/// function. An IOBuf can be converted into a managed buffer using the
/// `makeManaged` function.
///
/// # Layout
///
///     +-------+
///     | IOBuf |
///     +-------+
///      /
///     |            |-------- len -------|
///     v
///     +------------+--------------------+-----------+
///     | headroom   |        data        |  tailroom |
///     +------------+--------------------+-----------+
///     ^            ^                    ^
///     buf        data()               tail()
///
///     |------------------ capacity -----------------|
///
///
/// # Sharing
///
/// This type manages a reference counted buffer, allowing multiple IOBuf
/// sharing the same underlying buffer, each one pointing to (possibily)
/// different parts of the buffer. E.g. a server may have multiple HTTP
/// requests in flight for the same connection, with all their contents
/// written in a single buffer, but have different IOBufs for each one.
///
/// This doesn't matter if the code only reads from the buffer. However,
/// if the code writes to the buffer, you should first ensure that no other
/// IOBuf points to the same buffer, using `unshare`.
///
/// # Cloning
///
/// As with other collections, this type can't be simply copied around.
/// Temporary copies are fine, but any "owned" storage needs to explicitly
/// clone the buffer with `clone`. The method is responsible for incrementing
/// the reference count of the buffer. Failing to clone when needed can
/// lead to use-after-free bugs.
///
/// When you don't need a owned buffer anymore, it should be deinitialized
/// with `deinit`. It is responsible for decrementing the reference count
/// and failing to do so can lead to memory leaks.
///
/// # Allocators
///
/// Any method that may resoult in an allocation explicitly takes an allocator
/// parameter, but the allocator is stored in order to free the buffer when
/// the last owner is deinitialized. If IOBufs are shared between threads, this
/// may result in the allocator being used from another thread. In this case,
/// ensure the allocator free is thread-safe.
///
/// # Inspirations
///
/// This type is largely inspired by Meta's Folly library type with the same name.
/// Although the API was changed quite a bit, as their implementation largely
/// depends on a global allocator. Mainly, the IOBUf chain feature was removed
/// and is implemented in another type.
const std = @import("std");

const IOBuf = @This();

const sentinel_buf: [*]u8 = &[_]u8{};

/// Pointer in `buf` to where the data referenced by this IOBuf starts.
_data: [*]u8 = sentinel_buf,
/// The length of the data in this IOBuf.
len: usize = 0,
/// The capacity of this IOBuf, i.e. the length of `buf`.
capacity: usize = 0,
/// The allocated buffer for this IOBuf and any other instance that shares it.
///
/// Only the range referenced by this IOBuf is guaranteed to be valid.
buf: [*]u8 = sentinel_buf,

/// Information about how the buffer is shared with other instances and how
/// the buffer is managed.
///
/// If null, the buffer is managed by the caller.
///
/// In a managed buffer. this is allocated right after `buf`, improving locality and
/// allowing us to use a single allocation.
_shared_info: ?*SharedInfo = null,

/// Initializes the IOBuf, allocating a buffer of at least `capacity` bytes.
pub fn init(buf: *IOBuf, capacity: usize, allocator: std.mem.Allocator) !void {
    buf.unref();
    const good_size = goodAllocBufSize(capacity);
    const allocated_buf = try allocator.alloc(u8, good_size);

    buf.buf = allocated_buf.ptr;
    buf._data = buf.buf;
    buf.len = 0;
    buf.capacity = @as(usize, @intCast(allocated_buf.len)) - @sizeOf(SharedInfo);
    buf._shared_info = @alignCast(@ptrCast(buf.buf + buf.capacity));
    buf._shared_info.?.* = .{ .allocator = allocator };
}

/// Initializes the IOBuf with a user-provided buffer.
///
/// The caller is responsible for freeing the buffer after deinitializing the IOBuf.
pub fn initUnmanaged(buf: *IOBuf, user_buf: []u8) void {
    buf.unref();
    buf._data = user_buf.ptr;
    buf.buf = user_buf.ptr;
    buf.len = user_buf.len;
    buf.capacity = @intCast(user_buf.len);
    buf._shared_info = null;
}

/// Initializes the IOBuf by copying a user-provided buffer into its own buffer.
pub fn dupe(buf: *IOBuf, user_buf: []const u8, allocator: std.mem.Allocator) !void {
    try buf.dupeWithHeadroomAndTailroom(user_buf, 0, 0, allocator);
}

/// Initializes the IOBuf by copying a user-provided buffer into its own buffer.
///
/// Ensures we have the given headroom before the initialized buffer.
pub fn dupeWithHeadroom(buf: *IOBuf, user_buf: []const u8, _headroom: usize, allocator: std.mem.Allocator) !void {
    try buf.dupeWithHeadroomAndTailroom(user_buf, _headroom, 0, allocator);
}
/// Initializes the IOBuf by copying a user-provided buffer into its own buffer.
///
/// Ensures we have the given tailroom after the initialized buffer.
pub fn dupeWithTailroom(buf: *IOBuf, user_buf: []const u8, _tailroom: usize, allocator: std.mem.Allocator) !void {
    try buf.dupeWithHeadroomAndTailroom(user_buf, 0, _tailroom, allocator);
}

/// Initializes the IOBuf by copying a user-provided buffer into its own buffer.
///
/// Ensures we have the given headroom and tailroom before and after the initialized buffer.
pub fn dupeWithHeadroomAndTailroom(buf: *IOBuf, user_buf: []const u8, _headroom: usize, _tailroom: usize, allocator: std.mem.Allocator) !void {
    buf.unref();
    var min_cap = try std.math.add(usize, user_buf.len, _headroom);
    min_cap = try std.math.add(usize, min_cap, _tailroom);

    try buf.init(min_cap, allocator);
    buf._data = buf.buf + _headroom;
    buf.len = user_buf.len;

    @memcpy(buf.writableData(), user_buf);
}

pub fn clear(buf: *IOBuf) void {
    buf.len = 0;
    buf._data = buf.buf;
}

/// Deinitializes the IOBuf.
pub fn deinit(buf: *IOBuf) void {
    buf.unref();
    buf.* = .{};
}

/// Make a shallow clone of this IOBuf.
pub fn clone(buf: *const IOBuf) IOBuf {
    buf.ref();
    return buf.*;
}

/// Returns a new IOBuf that references part of the data of this IOBuf.
pub fn range(buf: *const IOBuf, start: usize, end: usize) IOBuf {
    if (buf.len < start) @panic("out of bounds start for IOBuf.range");
    if (buf.len < end) @panic("out of bounds end for IOBuf.range");
    if (end < start) @panic("end smaller than start for IOBuf.range");

    var new = buf.clone();
    new._data += start;
    new.len = end - start;

    return new;
}

/// The data in this IOBuf.
pub fn data(buf: *const IOBuf) []const u8 {
    return buf._data[0..buf.len];
}

/// The data in this IOBuf.
///
/// Note that accessing shared data as writable is undefined behavior, this
/// is checked if internal checks are enabled.
pub fn writableData(buf: *IOBuf) []u8 {
    buf.assertUnshared();
    return buf._data[0..buf.len];
}

/// The amount of space available in the tail of the buffer.
pub fn tailroom(buf: *const IOBuf) usize {
    return buf.capacity - buf.len - buf.headroom();
}

/// The remaining buffer after the data in this IOBuf.
///
/// Note that accessing shared data as writable is undefined behavior, this
/// is checked if internal checks are enabled.
pub fn writableTail(buf: *IOBuf) []u8 {
    buf.assertUnshared();
    return buf.buf[buf.headroom() + buf.len .. buf.capacity];
}

/// The amount of space available in the head of the buffer.
pub fn headroom(buf: *const IOBuf) usize {
    return @intFromPtr(buf._data) - @intFromPtr(buf.buf);
}

/// The remaining buffer before the data in this IOBuf.
///
/// Note that accessing shared data as writable is undefined behavior, this
/// is checked if internal checks are enabled.
pub fn writableHead(buf: *IOBuf) []u8 {
    buf.assertUnshared();
    return buf.buf[0..buf.headroom()];
}

/// The whole allocated buffer for this IOBuf.
///
/// Note that accessing shared data as writable is undefined behavior, this
/// is checked if internal checks are enabled.
pub fn writableBuffer(buf: *IOBuf) []u8 {
    buf.assertUnshared();
    return buf.buf[0..buf.capacity];
}

/// Adjusts the buffer after writing `amount` bytes into the tail.
///
/// The caller is responsible for making sure there is enough space and that
/// the data in the range is valid.
pub fn append(buf: *IOBuf, amount: usize) void {
    std.debug.assert(amount <= buf.tailroom());

    buf.len += amount;
}

/// Adjusts the buffer after writing `amount` bytes into the head.
///
/// The caller is responsible for making sure there is enough space and that
/// the data in the range is valid.
pub fn prepend(buf: *IOBuf, amount: usize) void {
    std.debug.assert(amount <= buf.headroom());

    buf._data -= amount;
    buf.len += amount;
}

/// Adjusts the buffer by adding `amount` to the head's end.
///
/// No data is copied: the data in the range is ignored.
pub fn trimStart(buf: *IOBuf, amount: usize) void {
    std.debug.assert(amount <= buf.capacity - buf.headroom());

    buf._data += amount;
    buf.len -|= amount;
}

/// Adjusts the buffer by adding `amount` to the tail's start.
pub fn trimEnd(buf: *IOBuf, amount: usize) void {
    std.debug.assert(amount <= buf.len);

    buf.len -= amount;
}

/// Append the given slice to the buffer.
///
/// Assumes (and is safety-checked) that there is enough room in the buffer.
pub fn appendSlice(buf: *IOBuf, slice: []const u8) void {
    std.debug.assert(buf.tailroom() >= slice.len);
    buf.assertUnshared();

    @memcpy(buf.writableTail().ptr, slice);
    buf.append(slice.len);
}

/// Append the given slice to the buffer.
///
/// Assumes (and is safety-checked) that there is enough room in the buffer.
pub fn prependSlice(buf: *IOBuf, slice: []const u8) void {
    std.debug.assert(buf.headroom() >= slice.len);
    buf.assertUnshared();

    @memcpy(buf._data - slice.len, slice);
    buf.prepend(slice.len);
}

/// If this IOBuf is shared with another instance.
pub fn isShared(buf: *const IOBuf) bool {
    return if (buf._shared_info) |si| si.ref_count.load(.acquire) > 1 else false;
}

/// If this IOBuf has ownership of its buffer.
pub fn isManaged(buf: *const IOBuf) bool {
    return buf._shared_info != null;
}

/// If the IOBuf holds an user-managed buffer, copies the data into its own buffer.
pub fn makeManaged(buf: *IOBuf, allocator: std.mem.Allocator) !void {
    if (buf.isManaged()) return;

    try buf.dupe(buf.data(), allocator);
}

/// Ensures this IOBuf shares no data with any other IOBuf.
///
/// This should be called before writing to the buffer.
pub fn unshare(buf: *IOBuf, allocator: std.mem.Allocator) !void {
    if (buf.isShared()) try buf.unshareSlow(allocator);
}

/// Ensures that the buffer has enough space in the head and tail.
///
/// If the buffer is shared, it will be unshared. This method may require a reallocation of the buffer,
/// which will use the same allocator as passed during initialization.
///
/// This cannot be called with an unmanaged IOBuf.
pub fn reserve(buf: *IOBuf, min_headroom: usize, min_tailroom: usize) !void {
    const needs_realloc = b: {
        if (min_headroom <= buf.headroom() and min_tailroom <= buf.tailroom()) break :b false;
        if (buf.len == 0 and min_headroom + min_tailroom <= buf.capacity) {
            buf._data = buf.buf + min_headroom;
            break :b false;
        }

        break :b true;
    };

    if (needs_realloc)
        try buf.reserveSlow(min_headroom, min_tailroom)
    else if (buf._shared_info) |si|
        try buf.unshare(si.allocator)
    else
        @panic("cannot call IOBuf.reserve with an unmanaged buffer");
}

fn unshareSlow(buf: *IOBuf, allocator: std.mem.Allocator) !void {
    var tmp = buf.*;
    try tmp.dupeWithHeadroomAndTailroom(buf.data(), buf.headroom(), buf.tailroom(), allocator);
    buf.unref();

    buf.buf = tmp.buf;
    buf.capacity = tmp.capacity;
    buf._shared_info = tmp._shared_info;
}

fn reserveSlow(buf: *IOBuf, min_headroom: usize, min_tailroom: usize) !void {
    var min_cap = try std.math.add(usize, buf.len, min_headroom);
    min_cap = try std.math.add(usize, min_cap, min_tailroom);

    const si = buf._shared_info.?.*;
    const buf_headroom = buf.headroom();

    // We already have enough room in the buffer, thus we can just copy data around.
    if (min_headroom + min_tailroom <= buf_headroom + buf.tailroom()) {
        const new_data = buf.buf[min_headroom .. min_headroom + buf.len];
        switch (std.math.order(min_headroom, buf_headroom)) {
            .lt => std.mem.copyForwards(u8, new_data, buf.data()),
            .gt => std.mem.copyBackwards(u8, new_data, buf.data()),
            // No need to copy if the data is in the same place.
            .eq => return,
        }

        buf._data = buf.buf + min_headroom;
        return;
    }

    var new_capacity: usize = 0;
    var new_buf: ?[*]u8 = null;

    // If we have enough headroom, but need more tailroom, we can try to expand the buffer.
    if (min_headroom <= buf_headroom and min_tailroom > buf.tailroom()) {
        const head_slack = buf_headroom - min_headroom;
        const copy_slack = buf.capacity - buf.len;

        new_capacity = goodAllocBufSize(min_cap + head_slack);

        // From Folly: prevent wasting memory with unneeded headroom, assuming tailroom is more useful.
        // Arbitrarily assume 25% of wasted capacity to be the threshold.
        //
        // Also, if we have too much wasted headroom, but the overhead is not too large, we also try to realloc,
        if (4 * head_slack <= new_capacity or 2 * copy_slack <= buf.len) {
            if (si.allocator.resize(buf.writableBuffer(), new_capacity)) {
                new_buf = buf.buf;
            }
        }
    } else {
        new_capacity = goodAllocBufSize(min_cap);
    }

    // Either we don't have enough headroom, have too much wasted memory or the allocator couldn't resize in-place.
    // Fallback to a alloc/copy/free.
    if (new_buf == null) {
        const allocated_buf = try si.allocator.alloc(u8, new_capacity);
        @memcpy(allocated_buf[min_headroom .. min_headroom + buf.len], buf.data());
        new_buf = allocated_buf.ptr;
        new_capacity = allocated_buf.len;
        si.freeBuffer(buf.buf, buf.capacity);
    }

    std.debug.assert(new_buf != null);

    buf.buf = new_buf.?;
    buf.capacity = new_capacity - @sizeOf(SharedInfo);
    buf._shared_info = @alignCast(@ptrCast(buf.buf + buf.capacity));
    buf._shared_info.?.* = si;

    return;
}

fn ref(buf: *const IOBuf) void {
    const si = buf._shared_info orelse return;
    _ = si.incRef();
}

fn unref(buf: *const IOBuf) void {
    // If there is no shared_info, the buffer is managed by the caller.
    const si = buf._shared_info orelse return;

    // PERF: Avoid a RMW if the ref count is 1.
    // As we are the last owner, and in the process of deinitialization, this is safe.
    if (si.isShared() and si.decRef() > 1) return;

    si.freeBuffer(buf.buf, buf.capacity);
}

inline fn assertUnshared(buf: *const IOBuf) void {
    std.debug.assert(!buf.isShared());
}

fn goodAllocBufSize(capacity: usize) usize {
    const shared_info_alignment = @alignOf(SharedInfo);

    // We will allocate shared info at the end so we can free directly using `buf`.
    var min_cap = capacity + @sizeOf(SharedInfo);
    // Ensures that shared info will be properly aligned.
    min_cap = (min_cap + shared_info_alignment - 1) & ~@as(usize, shared_info_alignment - 1);

    return std.math.ceilPowerOfTwo(usize, min_cap) catch min_cap;
}

const SharedInfo = struct {
    ref_count: std.atomic.Value(u32) = .{ .raw = 1 },
    allocator: std.mem.Allocator = undefined,

    inline fn isShared(self: *const @This()) bool {
        return self.ref_count.load(.acquire) > 1;
    }

    inline fn incRef(self: *@This()) u32 {
        return self.ref_count.fetchAdd(1, .acquire);
    }

    inline fn decRef(self: *@This()) u32 {
        return self.ref_count.fetchSub(1, .acquire);
    }

    inline fn freeBuffer(self: *const @This(), buf: [*]u8, cap: usize) void {
        self.allocator.free(buf[0 .. cap + @sizeOf(@This())]);
    }
};

test "IOBuf - basic" {
    var buf: IOBuf = .{};
    try buf.init(128, std.testing.allocator);
    defer buf.deinit();

    try std.testing.expect(buf.capacity >= 128);
    try std.testing.expectEqual(0, buf.headroom());
    try std.testing.expectEqual(0, buf.len);
    try std.testing.expectEqual(buf.capacity, buf.tailroom());

    buf.trimStart(10);
    buf.appendSlice("world");
    try std.testing.expectEqual(10, buf.headroom());
    try std.testing.expectEqual(5, buf.len);
    try std.testing.expectEqual(buf.capacity - 15, buf.tailroom());
    try std.testing.expectEqualStrings("world", buf.data());

    buf.prependSlice("hello ");
    try std.testing.expectEqual(4, buf.headroom());
    try std.testing.expectEqual(11, buf.len);
    try std.testing.expectEqual(buf.capacity - 15, buf.tailroom());

    try std.testing.expectEqualStrings("hello world", buf.data());

    buf.clear();
    try std.testing.expectEqual(0, buf.headroom());
    try std.testing.expectEqual(0, buf.len);
    try std.testing.expectEqual(buf.capacity, buf.tailroom());
}

test "alloc capacities" {
    var buf: IOBuf = .{};

    for (0..1234) |i| {
        try buf.init(i, std.testing.allocator);
        buf.deinit();
    }

    for ([_]usize{ 9000, 1048575, 1048576, 1048577 }) |i| {
        try buf.init(i, std.testing.allocator);
        buf.deinit();
    }
}

test "dupe" {
    var buf: IOBuf = .{};

    try buf.dupeWithHeadroomAndTailroom("hello world", 10, 10, std.testing.allocator);
    defer buf.deinit();

    try std.testing.expectEqual(10, buf.headroom());
    try std.testing.expect(buf.tailroom() >= 10);
    try std.testing.expectEqual(11, buf.len);
    try std.testing.expectEqualStrings("hello world", buf.data());
}

test "range" {
    var buf: IOBuf = .{};

    try buf.dupe("hello world", std.testing.allocator);
    defer buf.deinit();

    var b = buf.range(3, 9);
    defer b.deinit();

    try std.testing.expectEqualStrings("lo wor", b.data());
}

test "reserve" {
    var buf: IOBuf = .{};

    // case 1 - enough space and is empty.
    {
        try buf.init(128, std.testing.allocator);
        defer buf.deinit();

        const orig_buf = buf.buf;
        try buf.reserve(10, 80);
        try std.testing.expectEqual(orig_buf, buf.buf);
        try std.testing.expectEqual(10, buf.headroom());
    }

    // case 2 - enough space no reallocation.
    {
        try buf.init(200, std.testing.allocator);
        defer buf.deinit();

        buf.append(10);
        try std.testing.expectEqual(0, buf.headroom());
        try std.testing.expectEqual(10, buf.len);

        const orig_buf = buf.buf;
        try buf.reserve(1, 180);
        try std.testing.expectEqual(1, buf.headroom());
        try std.testing.expectEqual(orig_buf, buf.buf);
        try std.testing.expectEqual(orig_buf + 1, buf._data);
    }

    // case 3 - reallocate if not enough space
    {
        try buf.init(200, std.testing.allocator);
        defer buf.deinit();

        const orig_buf = buf.buf;
        try buf.reserve(100, 4 * 1024);

        try std.testing.expect(buf.buf != orig_buf);
    }
}
