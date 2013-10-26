#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '../util.rb'))
require  File.expand_path(File.join(drchord_dir, '../node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '../chord.rb'))
require 'drb/drb'

module DRChord
  class DHTShell
    INTERVAL = 1

    def initialize(options)
      @node = DRbObject::new_with_uri(options[:node])
    end

    def run
      Thread.new do
        loop do
          alive_monitoring
          sleep INTERVAL
        end
      end

      begin
        loop { accept_commands }
      rescue Interrupt
        stop
      end
    end

    def stop
      puts "Closing connection..."
      exit
    end

    private
    def alive_monitoring
      begin
        @node.active?
      rescue DRb::DRbConnError
        puts "Error: Connection failed - #{@node.__drburi}"; exit
      end
    end

    def accept_commands
      print "\n> "

      input = STDIN.gets.split
      cmd = input.shift
      args = input

      case cmd
      when "status"
        Util.print_node_info(@node.chord, @node.dhash)
      when "put"; put(args)
      when "get"; get(args)
      when "delete", "remove"; delete(args)
      when "exit"
        stop
      when "help", "h", "?"
        puts "Command list:"
        puts "  status"
        puts "  put"
        puts "  delete"
        puts "  help"
        puts "  exit"
      else
        puts "Error: No such command - #{cmd}" if cmd.nil? == false && cmd.length > 0
      end
    end

    def put(args)
      ret = @node.dhash.put(args[0], args[1])
      puts ret
    end

    def get(args)
      args.each do |arg|
        value = @node.dhash.get(arg)
        if value == nil || value == false
          puts "Error: Key not found. (#{arg})"
        else
          puts "Value: #{value}"
        end
      end
    end

    def delete(args)
      args.each do |arg|
        ret = @node.dhash.delete(arg)
        if ret == nil || ret == false
          puts "Error: Key not found. (#{arg})"
        else
          puts "Key & Value deleted. - (#{arg})"
        end
      end
    end
  end
end
