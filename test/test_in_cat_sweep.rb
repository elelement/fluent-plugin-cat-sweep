require_relative 'helper'
require 'rr'
require 'fluent/input'
require 'fluent/plugin/in_cat_sweep'

class CatSweepInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.mkdir_p(TMP_DIR_FROM)
    FileUtils.mkdir_p(TMP_DIR_TO)
  end

  def teardown
    FileUtils.rm_r(TMP_DIR_FROM)
    FileUtils.rm_r(TMP_DIR_TO)
  end

  TMP_DIR_FROM = '/tmp/fluent_plugin_test_in_cat_sweep_from'
  TMP_DIR_TO   = '/tmp/fluent_plugin_test_in_cat_sweep_to'

  CONFIG_BASE = %[
    file_path_with_glob #{TMP_DIR_FROM}/*
    run_interval 0.05
  ]

  CONFIG_MINIMUM_REQUIRED =
    if current_fluent_version < fluent_version('0.12.0')
      CONFIG_BASE + %[
        format tsv
        keys ""
        waiting_seconds 5
      ]
    else
      CONFIG_BASE + %[
        format tsv
        waiting_seconds 5
      ]
    end

  CONFIG_MINIMUM_ALL_MODE =
    CONFIG_BASE + %[
      format none
      waiting_seconds 5
      cat_mode all
    ]

  def create_driver(conf, use_v1 = true)
    driver = Fluent::Test::InputTestDriver.new(Fluent::CatSweepInput)
    if current_fluent_version < fluent_version('0.10.51')
      driver.configure(conf)
    else
      driver.configure(conf, use_v1)
    end
    driver
  end

  def test_required_configure
    assert_raise(Fluent::ConfigError) do
      d = create_driver(%[])
    end

    assert_raise(Fluent::ConfigError) do
      d = create_driver(CONFIG_BASE)
    end

    assert_raise(Fluent::ConfigError) do
      d = create_driver(CONFIG_BASE + %[format tsv])
    end

    d = create_driver(CONFIG_MINIMUM_REQUIRED)

    assert_equal "#{TMP_DIR_FROM}/*", d.instance.instance_variable_get(:@file_path_with_glob)
    assert_equal 'tsv', d.instance.instance_variable_get(:@format)
    assert_equal 5, d.instance.instance_variable_get(:@waiting_seconds)
  end

  def test_configure_cat_mode
    d = create_driver(CONFIG_MINIMUM_REQUIRED)
    assert { 'line' == d.instance.cat_mode }

    d = create_driver(CONFIG_MINIMUM_REQUIRED + %[cat_mode stream])
    assert { 'stream' == d.instance.cat_mode }

    d = create_driver(CONFIG_MINIMUM_ALL_MODE)
    assert { 'all' == d.instance.cat_mode }
  end

  def compare_test_result(emits, tests)
    emits.each_index do |i|
      assert { tests[i]['expected'] == emits[i][2]['message'] }
    end
  end

  TEST_CASES =
    {
      'none' => [
        {'msg' => "tcptest1\n", 'expected' => 'tcptest1'},
        {'msg' => "tcptest2\n", 'expected' => 'tcptest2'}
      ],
      'tsv' => [
        {'msg' => "t.e.s.t.1\t12345\ttcptest1\t{\"json\":1}\n", 'expected' => '{"json":1}'},
        {'msg' => "t.e.s.t.2\t54321\ttcptest2\t{\"json\":\"char\"}\n", 'expected' => '{"json":"char"}'}
      ],
      'json' => [
        {'msg' => {'k' => 123, 'message' => 'tcptest1'}.to_json + "\n", 'expected' => 'tcptest1'},
        {'msg' => {'k' => 'tcptest2', 'message' => 456}.to_json + "\n", 'expected' => 456}
      ]
    }

  # Only for "line by line" cat methods
  ['line', 'stream'].each do |cat_mode|
    TEST_CASES.each do |format, test_cases|
      test_case_name = "test_msg_process_#{format}cat_mode#{cat_mode}"
      define_method(test_case_name) do
        File.open("#{TMP_DIR_FROM}/#{test_case_name}", 'w') do |io|
          test_cases.each do |test|
            io.write(test['msg'])
          end
        end

        d = create_driver(CONFIG_BASE + %[
          format #{format}
          cat_mode #{cat_mode}
          waiting_seconds 0
          keys hdfs_path,unixtimestamp,label,message
          ])
        d.run

        compare_test_result(d.emits, test_cases)
        assert { Dir.glob("#{TMP_DIR_FROM}/#{test_case_name}*").empty? }
      end
    end
  end

  TEST_CASES_ALL_MODE =
    {
      'raw_json' => [
        {'msg' => {'k' => 'tcptest2', 'message' => 456}.to_json + "\n", 'expected' => {'k' => 'tcptest2', 'message' => 456}.to_json},
      ],
      'multiline_text' => [
        {'msg' => "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt \n
          ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris \n
          nisi ut aliquip ex ea commodo consequat.\n",
         'expected' => "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt \n
          ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris \n
          nisi ut aliquip ex ea commodo consequat."
        }
      ]
    }

  print "Test cases for whole file content (always format none)\n"
  TEST_CASES_ALL_MODE.each do |test_name, test_cases|
    test_case_name = "test_msg_process_#{test_name}_noneallall"
    define_method(test_case_name) do
      File.open("#{TMP_DIR_FROM}/#{test_case_name}", 'w') do |io|
        test_cases.each do |test|
          io.write(test['msg'])
        end
      end

      d = create_driver(CONFIG_BASE + %[
        format none
        cat_mode all
        waiting_seconds 0
        keys hdfs_path,unixtimestamp,label,message
        ])
      d.run

      d.emits.each_index do |i|
        assert { test_cases[i]['expected'] == d.emits[i][2]['message'] }
      end
      assert { Dir.glob("#{TMP_DIR_FROM}/#{test_case_name}*").empty? }
    end
  end


  def test_move_file
    format = 'tsv'
    test_cases =
      [
        {'msg' => "t.e.s.t.1\t12345\ttcptest1\t{\"json\":1}\n", 'expected' => '{"json":1}'},
        {'msg' => "t.e.s.t.2\t54321\ttcptest2\t{\"json\":\"char\"}\n", 'expected' => '{"json":"char"}'},
      ]

    File.open("#{TMP_DIR_FROM}/test_move_file", 'w') do |io|
      test_cases.each do |test|
        io.write(test['msg'])
      end
    end

    d = create_driver(CONFIG_BASE + %[
      format #{format}
      waiting_seconds 0
      keys hdfs_path,unixtimestamp,label,message
      move_to #{TMP_DIR_TO}
      ])
    d.run

    compare_test_result(d.emits, test_cases)

    assert(Dir.glob("#{TMP_DIR_FROM}/test_move_file*").empty?)
    assert_match(
      %r{\A#{TMP_DIR_TO}#{TMP_DIR_FROM}/test_move_file},
      Dir.glob("#{TMP_DIR_TO}#{TMP_DIR_FROM}/test_move_file*").first)
    assert_equal(
      test_cases.map{|t|t['msg']}.join.to_s,
      File.read(Dir.glob("#{TMP_DIR_TO}#{TMP_DIR_FROM}/test_move_file*").first))
  end

  def test_oneline_max_bytes
    format = 'tsv'
    test_cases =
      [
        {'msg' => "t.e.s.t.1\t12345\ttcptest1\t{\"json\":1}\n", 'expected' => '{"json":1}'},
        {'msg' => "t.e.s.t.2\t54321\ttcptest2\t{\"json\":\"char\"}\n", 'expected' => '{"json":"char"}'},
      ]

    File.open("#{TMP_DIR_FROM}/test_oneline_max_bytes", 'w') do |io|
      test_cases.each do |test|
        io.write(test['msg'])
      end
    end

    d = create_driver(CONFIG_BASE + %[
      format #{format}
      waiting_seconds 0
      keys hdfs_path,unixtimestamp,label,message
      move_to #{TMP_DIR_TO}
      oneline_max_bytes 1
      ])

    d.run

    assert_match(
      %r{\A#{TMP_DIR_FROM}/test_oneline_max_bytes.*\.error},
      Dir.glob("#{TMP_DIR_FROM}/test_oneline_max_bytes*").first)
      assert_equal(
        test_cases.map{|t|t['msg']}.join.to_s,
        File.read(Dir.glob("#{TMP_DIR_FROM}/test_oneline_max_bytes*.error").first))
  end
end

