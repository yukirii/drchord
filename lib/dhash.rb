#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '/replication_manager.rb'))
require "zlib"

module DRChord
  class DHash
    attr_reader :logger, :hash_table, :replication
    def initialize(chord, logger)
      @chord = chord
      @replication = ReplicationManager.new(chord, self)
      @logger = logger || Logger.new(STDERR)
      @hash_table = {}
    end

    def start(bootstrap = nil)
      logger.info "Ctrl-C to shutdown node"
      @chord_thread = @chord.start(bootstrap)
      @replication_thread = @replication.start
      begin
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
        @replication.create(id, value, @chord.successor_list)
        logger.info "#{@chord.info.uri("dhash")}: put key:#{key} value:#{value}"
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
          @replication.delete(@chord.info.id, id)
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
