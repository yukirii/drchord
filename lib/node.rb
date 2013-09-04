#!/usr/bin/env ruby
# encoding: utf-8

require 'zlib'

module DRChord
  class Node
    M = 32
    SLIST_SIZE = 3

    attr_accessor :ip, :port, :finger, :successor_list, :predecessor
    def initialize(options)
      options = default_options.merge(options)

      @ip = options[:ip]
      @port = options[:port]

      @finger = []
      @successor_list = []
      @predecessor = nil
    end

    def successor
      return @finger[0]
    end

    def successor=(node)
      @finger[0] = node
    end

    def id
      return Zlib.crc32("#{@ip}:#{@port}")
    end


    private
    def default_options
      return {:ip => '127.0.0.1', :port => 30000}
    end
  end
end
