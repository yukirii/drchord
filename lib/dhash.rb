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
      @logger = logger || Logger.new(STDERR)
      @chord = chord
      @replication = ReplicationManager.new(self, logger)
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

    def put(key, value, calculate_hash = true)
      return false if key == nil

      id = calculate_hash ? Zlib.crc32(key) : key
      successor_node = @chord.find_successor(id)
      if successor_node.id == @chord.info.id
        @hash_table.store(id, value)
        logger.debug "#{@chord.info.uri("dhash")}: stored key:#{key} value:#{value}"

        @replication.create(id, value)
        return true
      else
        begin
          return DRbObject::new_with_uri(successor_node.uri("dhash")).put(key, value, calculate_hash)
        rescue DRb::DRbConnError
          return false
        end
      end
    end

    def get(key)
      return false if key == nil

      id = Zlib.crc32(key)
      candidates_list = @chord.successor_candidates(id, 3)
      candidates_list = candidates_list.uniq
      successor_node = candidates_list.first

      if successor_node.id == @chord.id
        logger.debug "#{@chord.info.uri("dhash")}: get key:#{key}"
        ret = @hash_table.fetch(id, nil)

        if ret == nil || ret == false
          ret = request_to_candidates(key, candidates_list)
        end
        return ret == nil || ret == false ? false : ret
      else
        begin
          DRbObject::new_with_uri(successor_node.uri("dhash")).get(key)
        rescue DRb::DRbConnError
          candidates_list.shift
          return false if candidates_list.empty?
          successor_node = candidates_list.first
          retry
        end
      end
    end

    def get_local(key)
      id = Zlib.crc32(key)
      ret = @hash_table.fetch(id, nil)
      return ret != nil && ret != false ? ret : false
    end

    def delete(key)
      return false if key == nil

      id = Zlib.crc32(key)
      successor_node = @chord.find_successor(id)
      if successor_node.id == @chord.info.id
        ret = @hash_table.delete(id)
        unless ret.nil?
          @replication.delete(id)
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

    private
    def request_to_candidates(key, candidates_list)
      candidates_list.each do |candidate_node|
        if candidate_node.id != @chord.id
          begin
            ret = DRbObject::new_with_uri(candidate_node.uri("dhash")).get_local(key)
            return ret if ret != nil && ret != false
          rescue DRb::DRbConnError; next
          end
        end
      end
      return false
    end
  end
end
