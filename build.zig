const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    _ = b.addModule("iobuf", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // zig build check
    {
        const step = b.step("check", "check for semantic analysis errors");
        const check = b.addTest(.{
            .name = ".iobuf-check",
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = .Debug,
        });
        check.generated_bin = null;
        step.dependOn(&check.step);
    }

    // zig build test
    {
        const test_step = b.step("test", "run unit tests");
        const unit_tests = b.addTest(.{
            .name = "iobuf-unit-tests",
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        });
        test_step.dependOn(&b.addRunArtifact(unit_tests).step);
    }
}
