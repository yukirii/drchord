#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require "zlib"

module DRChord
  class DHash
    attr_reader :logger
    def initialize(chord, logger)
      @chord = chord
      @hash_table = {}

      @logger = logger || Logger.new(STDERR)
    end

    def start(bootstrap = nil)
      @chord.start(bootstrap)
    end

    def put(key, value)
      return false if key == nil

      id = Zlib.crc32(key)
      successor_node = @chord.find_successor(id)
      if successor_node.id == @chord.info.id
        @hash_table.store(id, value)
        logger.info "#{@chord.info.uri("dhash")}: put key:#{key} value:#{value}"
        # レプリカの作成
        return true
      else
        begin
          return DRbObject::new_with_uri(successor_node.uri("dhash")).put(key, value)
        rescue DRb::DRbConnError
          return false
        end
      end
    end

    def get(key)
      return false if key == nil

      id = Zlib.crc32(key)
      successor_node = @chord.find_successor(id)
      if successor_node.id == @chord.info.id
        logger.info "#{@chord.info.uri("dhash")}: get key:#{key}"
        ret = @hash_table.fetch(id, nil)
        if ret.nil?
          # hash_table にない場合 レプリカを探す
          return false
        else
          return ret
        end
      else
        begin
          return DRbObject::new_with_uri(successor_node.uri("dhash")).get(key)
        rescue DRb::DRbConnError
          return false
        end
      end
    end

    def delete(key)
      return false if key == nil

      id = Zlib.crc32(key)
      successor_node = @chord.find_successor(id)
      if successor_node.id == @chord.info.id
        ret = @hash_table.delete(id)
        unless ret.nil?
          # レプリカから削除する
          logger.info "#{@chord.info.uri("dhash")}: delete key:#{key}"
          return true
        else
          return false
        end
      else
        begin
          return DRbObject::new_with_uri(successor_node.uri("dhash")).delete(key)
        rescue DRb::DRbConnError
          return false
        end
      end
    end
  end
end
