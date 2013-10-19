#!/usr/bin/env ruby
# encoding: utf-8

require 'zlib'

module DRChord
  class NodeInformation
    attr_reader :ip, :port
    def initialize(ip, port)
      @ip = ip
      @port = port
    end

    def id
      return Zlib.crc32("#{@ip}:#{@port}")
    end

    def uri(arg = "chord")
      uri = "druby://#{@ip}:#{@port}"
      case arg
      when "chord"
        uri += "?chord"
      when "dhash"
        uri += "?dhash"
      else
        nil
      end
    end
  end
end
