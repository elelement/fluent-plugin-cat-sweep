# fluent-plugin-cat-sweep

[![Build Status](https://secure.travis-ci.org/elelement/fluent-plugin-cat-sweep.png?branch=master)](http://travis-ci.org/elelement/fluent-plugin-cat-sweep)

Fluentd plugin to read data from files and to remove or move after processing.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'fluent-plugin-cat-sweep'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-cat-sweep

## Basic Behavior

Assume that an application outputs logs into `/tmp/test` directory as

```
tmp/test
├── accesss.log.201509151611
├── accesss.log.201509151612
└── accesss.log.201509151613
```

in every one minute interval. 

This plugin watches the directory (`file_path_with_glob tmp/test/access.log.*`), and reads the contents and sweep (deafault: remove) for files whose mtime are passed in 60 seconds (can be configured with `waiting_seconds`).

Our assumption is that this mechanism should provide more durability than `in_tail` (batch read overcomes than streaming read). 

## Configuration

```
<source>
  type cat_sweep

  # Required. process files that match this pattern using glob.
  file_path_with_glob /tmp/test/file_*

  # Input pattern. It depends on Parser plugin
  format tsv
  keys xpath,access_time,label,payload

  # Required. process files that are older than this parameter(seconds).
  # [WARNING!!]: this plugin move or remove files even if the files open,
  # so this parameter is set as seconds that the application close files definitely.  
  waiting_seconds 60

  # Optional. default is file.cat_sweep
  tag test.input

  # Optional. processing files is renamed with this suffix. default is .processing
  processing_file_suffix .processing

  # Optional. error files is renamed with this suffix. default is .error
  error_file_suffix .err

  # Optional. line terminater. default is "\n"
  line_terminated_by ,

  # Optional. max bytes oneline can has. default 536870912 (512MB)
  oneline_max_bytes 128000

  # Optional. processed files are move to this directory.
  # default '/tmp'
  move_to /tmp/test_processed

  # Optional. this parameter indicated, `move_to` is ignored.
  # files that is processed are removed.
  # default is false.
  remove_after_processing true

  # Optional. default 5 seconds.
  run_interval 10

  # Optional. Choose the default emitting mode for a file: "line", "stream" or "all":
  # 1. line: emits the file line by line, each one as an independent message.
  # 2. stream: emits the entire file contents as an event, default emits each line as an event (each line produces an individual message).
  # 3. all: emits the whole file as a unique string message.
  #
  # Please note that buffer_chunk_limit must be larger than bytes in a file to be sent by buffered output plugins such as out_forward, out_s3.
  cat_mode stream
</source>
```

## ChangeLog

[CHANGELOG.md](CHANGELOG.md)

## Warning

* This plugin supports fluentd from v0.10.45
* The support for fluentd v0.10 will end near future
* The support for fluentd v0.14 is not yet

## Contributing

1. Fork it ( https://github.com/civitaspo/fluent-plugin-cat-sweep/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
