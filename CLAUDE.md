The goal of this project is to make a fast and responsive AWS S3 browser utility in Rust using Dear ImGui,
and as few external dependencies as possible. The current app uses a wgpu/winit backend and is built with Cargo.

We don't allow any edit operations on AWS S3, but it should let you browse through a bucket, even ones
with millions of objects stored in the same prefix. The user should see things load as the requests come in,
and there should never be a lag while HTTP requests finish.

The idea is that the GUI will always be fast and responsive.

When you need to run the executable, build it with Cargo and tell the user to run it. Don't just launch the
main executable in the background.
