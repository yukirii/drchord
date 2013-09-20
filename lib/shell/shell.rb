#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '../util.rb'))
require 'drb/drb'

module DRChord
  class DHTShell
    INTERVAL = 5

    def initialize(options)
      @node = DRbObject::new_with_uri(options[:node])
    end

    def run
      Thread.new do
        begin
          loop{ @node.active?; sleep INTERVAL }
        rescue DRb::DRbConnError
          puts "Error: Connection failed - #{@node.__drburi}"; exit
        end
      end

      begin
        loop do
          print "\n> "

          input = STDIN.gets.split
          cmd = input.shift
          args = input

          case cmd
          when "status"
            Util.print_node_info(@node)
          when "get"; get(args)
          when "put"; put(args)
          when "delete", "remove"; delete(args)
          when "exit"
            stop
          when "help", "h", "?"
            puts << "Command list:"
            puts "  status"
            puts "  put"
            puts "  delete"
            puts "  help"
            puts "  exit"
          else
            puts "Error: No such command - #{cmd}" if cmd.nil? == false && cmd.length > 0
          end
        end
      rescue Interrupt
        stop
      end
    end

    def stop
      puts "Closing connection..."
      exit
    end

    private
    def get(args)
      value = @node.get(args[0])
      if value == nil || value == false
        puts "Error: Key not found. (#{args[0]})"
      else
        puts "Value: #{value}"
      end
    end

    def put(args)
      ret = @node.put(args[0], args[1])
      puts ret
    end

    def delete(args)
      ret = @node.delete(args[0])
      if ret == nil || ret == false
        puts "Error: Key not found. (#{args[0]})"
      else
        puts "Value #{args[0]} deleted."
      end
    end
  end
end
