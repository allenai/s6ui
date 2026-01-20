# s6ui

`s6ui` is a fast GUI for browsing AWS S3 buckets. If you like `s5cmd`, then you will like `s6ui`.

<img width="1312" height="940" alt="Screenshot 2026-01-14 at 2 36 06 PM" src="https://github.com/user-attachments/assets/1c05b724-018d-43fc-8716-e06882f8b851" />


s6ui lets you browse an AWS S3 bucket with a lightweight GUI powered by [DearImGui](https://github.com/ocornut/imgui).
s6ui hides latency by prefetching things when you hover your cursor over them. By the time you click on something, it will load instantly.

There are some tools built-in to help with previewing large datasets. Click on a file
containing [Dolma documents](https://github.com/allenai/dolma), and you can quickly see a preview of the contents. .gz and .zstd decoders are included. You don't even need to wait to load a full 1GB file,
everything is streamed to make this as fast as possible. It's a quick way to see what's inside your bucket or dataset.

### How to intall from Homebrew (MacOSX)
```bash
brew tap allenai/s6ui
brew install s6ui
```

### How to build from source
Right now we support MacOSX, (soon Linux).

Install dependencies with `brew install glfw`
then just run `make`

### How to use

Just type `./s6ui` to launch it.

You can specify `./s6ui s3://my_bucket/path` to immediately jump to that path.

### AWS Authentication

s6ui supports multiple methods for AWS authentication:

#### Option 1: Static Credentials File

Create or edit `~/.aws/credentials` with your AWS access keys:

```
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

You can also define multiple profiles:

```
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY

[work]
aws_access_key_id = WORK_ACCESS_KEY_ID
aws_secret_access_key = WORK_SECRET_ACCESS_KEY
endpoint_url = https://custom-weka-server.org:9000
```

To use a specific profile, set the `AWS_PROFILE` environment variable:

```bash
AWS_PROFILE=work ./s6ui
```

Regions are auto-detected.

#### Option 2: AWS SSO Configuration

Configure AWS SSO using the AWS CLI:

```bash
aws configure sso
```

Follow the prompts to set up your SSO profile. This will create configuration in `~/.aws/config`.

To use an SSO profile, set the `AWS_PROFILE` environment variable:

```bash
AWS_PROFILE=my-sso-profile ./s6ui
```

s6ui will automatically handle SSO authentication and token refresh as needed.

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
