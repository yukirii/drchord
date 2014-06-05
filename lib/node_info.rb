#!/usr/bin/env ruby
# encoding: utf-8

require 'zlib'

module DRChord
  # ノードへの接続に必要な情報
  class NodeInformation
    attr_reader :ip, :port
    def initialize(ip, port)
      @ip = ip
      @port = port
    end

    # ノードの ID を返す
    # @return [Fixnum] ノードの IP:Port から計算したハッシュ値
    def id
      return Zlib.crc32("#{@ip}:#{@port}")
    end

    # ノードの URI を返す
    # @param [String] arg Front を通してアクセスするインスタンスを指定する
    # @return [String] 指定したインスタンスを表す URI
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
