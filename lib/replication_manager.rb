#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/chord.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '/dhash.rb'))
require  File.expand_path(File.join(drchord_dir, '/util.rb'))

module DRChord
  class ReplicationManager
    INTERVAL = 30
    SLIST_SIZE = 3
    NUMBER_OF_COPIES = 3

    attr_reader :logger
    def initialize(dhash, logger)
      @logger = logger || Logger.new(STDERR)
      @dhash = dhash
      @chord = @dhash.chord
      @chord.add_observer(self, :transfer)
    end

    def start
      @reput_thread = Thread.new do
        loop do
          sleep INTERVAL - 5 + rand(10)
          reput
        end
      end
    end

    def stop
      @reput_thread.kill
    end

    # 新規レプリカの配置処理
    def create(id, value)
      candidates_list = @chord.successor_candidates(id, NUMBER_OF_COPIES)
      candidates_list.each do |s|
        if s.id != @chord.id
          dhash = DRbObject::new_with_uri(s.uri("dhash"))
          dhash.hash_table = dhash.hash_table.merge({id => value})
        end
      end
    end

    # レプリカの削除
    def delete(id)
      candidates_list = @chord.successor_candidates(id, NUMBER_OF_COPIES)
      candidates_list.each do |s|
        if s.id != @chord.id
          dhash = DRbObject::new_with_uri(s.uri("dhash"))
          dhash.hash_table = dhash.hash_table.reject{|key, value| key == id }
        end
      end
    end

    # 加入時移譲
    def transfer
      cnt = 0
      while cnt < 3
        candidates_list = @chord.successor_candidates(@chord.id, NUMBER_OF_COPIES)
        succs_pred = DRbObject::new_with_uri(@chord.successor.uri).predecessor

        if succs_pred.nil?
          cnt += 1
          logger.debug "Key-Value transfer - Successor's predecessor is nil. retrying...(#{cnt})"
          sleep 3
        else
          pair = {}
          candidates_list.each do |n|
            dhash = DRbObject::new_with_uri(n.uri("dhash"))
            kv_pair = dhash.replication.request_kv_pair(succs_pred.id, @chord.id)
            pair = pair.merge(kv_pair)
          end
          @dhash.hash_table = @dhash.hash_table.merge(pair)
          logger.debug "Key-Value transfer - successful. "
          return true
        end
      end
      logger.debug "Key-Value transfer - failed. "
    end

    def request_kv_pair(pred, node_id)
      kv_pair = {}
      @dhash.hash_table.each do |key, value|
        if Util::betweenE(key, pred, node_id)
          kv_pair.store(key, value)
        end
      end
      return kv_pair
    end

    private
    # 自動再 put
    def reput
      return false if @chord.active? == false

      @dhash.hash_table.each do |key, value|
        if keys_owner?(key) == false
          @dhash.hash_table = @dhash.hash_table.reject{|k, v| k == key }
          logger.debug "#{@chord.info.uri("dhash")}: Delete key:#{key} value:#{value}"
        else
          @dhash.put(key, value, false)
          logger.debug "#{@chord.info.uri("dhash")}: reput key:#{key} value:#{value}"
        end
      end
    end

    def keys_owner?(key)
      if @chord.predecessor != nil && Util::betweenE(key, @chord.predecessor.id, @chord.id) == true
        return true
      end

      candidates_list = @chord.successor_candidates(key, NUMBER_OF_COPIES)
      candidates_list.each do |node|
        if node.id == @chord.id
          return true
          break
        end
      end
      return false
    end
  end
end
