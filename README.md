# s6ui

If `s5cmd` is a fast way to work with AWS S3 buckets via the command line, then `s6ui` is a fast way to do so via GUI.

<img width="1195" height="821" alt="image" src="https://github.com/user-attachments/assets/fa5d5723-bddd-43bb-96d3-fff9a8f423a1" />

s6ui lets you browse an AWS S3 bucket with a nice lightweight GUI. The idea is that requests are heavily prefetched, so browsing is
as instant as possible. Most of the time you just click on a prefix/folder and the contents shows up immediately.

There are also nice tools built around some of the things we do here at AI2, such as previewing large datasets. So, click on a file
containing [Dolma documents](https://github.com/allenai/dolma), and you can quickly see results. You don't even need to load a full 1GB file,
everything is streamed to make this as fast as possible.

### How to build
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
```

To use a specific profile, set the `AWS_PROFILE` environment variable:

```bash
AWS_PROFILE=work ./s6ui
```

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
