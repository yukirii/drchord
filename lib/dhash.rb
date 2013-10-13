#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '/replication_manager.rb'))
require "zlib"

module DRChord
  class DHash
    attr_reader :logger, :chord, :replication
    attr_accessor :hash_table
    def initialize(chord, logger)
      @chord = chord
      @replication = ReplicationManager.new(self)
      @logger = logger || Logger.new(STDERR)
      @hash_table = {}
    end

    def start(bootstrap = nil)
      logger.info "Ctrl-C to shutdown node"
      begin
        @chord_thread = @chord.start(bootstrap)
        @replication_thread = @replication.start
        @chord_thread.join
        @replication_thread.join
      rescue Interrupt
        shutdown
      end
    end

    def shutdown
      logger.info "going to shutdown..."
      @replication.stop
      @chord.leave
    end

    def put(key, value)
      return false if key == nil

      id = Zlib.crc32(key)
      successor_node = @chord.find_successor(id)
      if successor_node.id == @chord.info.id
        @hash_table.store(id, value)
        @replication.create(id, value)
        logger.debug "#{@chord.info.uri("dhash")}: put key:#{key} value:#{value}"
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
        logger.debug "#{@chord.info.uri("dhash")}: get key:#{key}"
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
          @replication.delete(@chord.info.id, id)
          logger.debug "#{@chord.info.uri("dhash")}: delete key:#{key}"
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

    def insert_entries(entries)
      @hash_table.merge!(entries)
    end

    def request_kv_pair(node_id)
      kv_pair = {}
      @hash_table.each do |key, value|
        if Util::betweenE(key, @chord.predecessor.id, node_id)
          kv_pair.store(key, value)
        end
      end
      return kv_pair
    end
  end
end
