# s6ui

`s6ui` is a fast Rust GUI for browsing AWS S3 buckets. If you like `s5cmd`, then you will like `s6ui`.

<img width="1312" height="940" alt="Screenshot 2026-01-14 at 2 36 06 PM" src="https://github.com/user-attachments/assets/1c05b724-018d-43fc-8716-e06882f8b851" />

The app is built with [Dear ImGui](https://github.com/ocornut/imgui), `wgpu`, and `winit`.
It hides latency by prefetching bucket listings and previews when you hover over entries, so navigation feels immediate even against large prefixes.

There are built-in tools for previewing large datasets. `.gz` and `.zstd` files are decoded on the fly, and previews are streamed so you do not need to wait for a full multi-GB object download before seeing useful content.

### Install from Homebrew

```bash
brew tap allenai/s6ui
brew install s6ui
```

### Build from source

Install a current Rust toolchain, then build or run the app from the repository root:

```bash
cargo build --release
cargo run --release
```

On Ubuntu, install the same native packages used in CI before building:

```bash
sudo apt-get update
sudo apt-get install -y pkg-config libssl-dev libx11-xcb-dev libxcursor-dev libxi-dev libxinerama-dev libxrandr-dev libxkbcommon-dev libwayland-dev
```

### Usage

Run `target/release/s6ui` after `cargo build --release`, or just use:

```bash
cargo run --release -- s3://my-bucket/path
```

You can pass an initial S3 path to jump directly into a bucket or prefix.

### AWS authentication

`s6ui` supports multiple AWS authentication methods.

#### Option 1: Static credentials file

Create or edit `~/.aws/credentials` with your AWS access keys:

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

You can also define multiple profiles:

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY

[work]
aws_access_key_id = WORK_ACCESS_KEY_ID
aws_secret_access_key = WORK_SECRET_ACCESS_KEY
endpoint_url = https://custom-weka-server.org:9000
```

To use a specific profile, set `AWS_PROFILE`:

```bash
AWS_PROFILE=work cargo run --release
```

Regions are auto-detected.

#### Option 2: AWS SSO configuration

Configure AWS SSO using the AWS CLI:

```bash
aws configure sso
```

Follow the prompts to set up your SSO profile. This will create configuration in `~/.aws/config`.

To use an SSO profile, set `AWS_PROFILE`:

```bash
AWS_PROFILE=my-sso-profile cargo run --release
```

`s6ui` will automatically handle SSO authentication and token refresh as needed.

<!-- start team -->

**s6ui** is developed and maintained by the AllenNLP team, backed by [the Allen Institute for Artificial Intelligence (AI2)](https://allenai.org/).
AI2 is a non-profit institute with the mission to contribute to humanity through high-impact AI research and engineering.
To learn more about who specifically contributed to this codebase, see [our contributors](https://github.com/allenai/s6ui/graphs/contributors) page.

<!-- end team -->

## License

<!-- start license -->

**s6ui** is licensed under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0).
A full copy of the license can be found [on GitHub](https://github.com/allenai/s6ui/blob/main/LICENSE).

<!-- end license -->
