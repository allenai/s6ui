# s3v

If `s5cmd` is a fast way to work with AWS S3 buckets via the command line, then `s3v` is a fast way to do so via GUI.

<img width="1195" height="821" alt="image" src="https://github.com/user-attachments/assets/fa5d5723-bddd-43bb-96d3-fff9a8f423a1" />

s3v lets you browse an AWS S3 bucket with a nice lightweight GUI. The idea is that requests are heavily prefetched, so browsing is 
as instant as possible. Most of the time you just click on a prefix/folder and the contents shows up immediately.

There are also nice tools built around some of the things we do here at AI2, such as previewing large datasets. So, click on a file
containing [Dolma documents](https://github.com/allenai/dolma), and you can quickly see results. You don't even need to load a full 1GB file,
everything is streamed to make this as fast as possible.

### How to build
Right now we support MacOSX, (soon Linux).

Install dependencies with `brew install glfw`
then just run `make`

### How to use

Just type `./s3v` to launch it.

You can specify `./s3v s3://my_bucket/path` to immediately jump to that path.

<!-- start team -->

**s3v** is developed and maintained by the AllenNLP team, backed by [the Allen Institute for Artificial Intelligence (AI2)](https://allenai.org/).
AI2 is a non-profit institute with the mission to contribute to humanity through high-impact AI research and engineering.
To learn more about who specifically contributed to this codebase, see [our contributors](https://github.com/allenai/s3v/graphs/contributors) page.

<!-- end team -->

## License

<!-- start license -->

**s3v** is licensed under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0).
A full copy of the license can be found [on GitHub](https://github.com/allenai/s3v/blob/main/LICENSE).

<!-- end license -->
