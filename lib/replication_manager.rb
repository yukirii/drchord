#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '/dhash.rb'))
require  File.expand_path(File.join(drchord_dir, '/util.rb'))

module DRChord
  class ReplicationManager
    INTERVAL = 10
    SLIST_SIZE = 3
    NUMBER_OF_COPIES = 3

    attr_reader :replicas
    def initialize(dhash)
      @dhash = dhash
      @chord = @dhash.chord
      @chord.add_observer(self, :transfer)

      @replicas = {}
    end

    def start
      @replica_thread = Thread.new do
        loop do
          if @chord.active?
          end
          sleep INTERVAL
        end
      end
    end

    def stop
      @replica_thread.kill
    end

    def insert(node_id, hash)
      return if @chord.info.id == node_id
      if @replicas[node_id].nil?
        @replicas[node_id] = hash
      else
        @replicas[node_id].merge!(hash)
      end
    end

    def delete(node_id, replica = nil)
      if replica.nil?
        @replicas.reject!{|id, hash| id == node_id }
      else
        unless @replicas[node_id].nil?
          @replicas[node_id].reject!{|key, value| Util.betweenE(key, node_id, replica) }
        end
      end
    end

    # 新規レプリカの配置処理
    def create(id, value)
      candidates_list = @chord.successor_candidates(id, NUMBER_OF_COPIES)
      candidates_list.each do |s|
        p s.uri
        dhash = DRbObject::new_with_uri(s.uri("dhash"))
        dhash.replication.insert(@chord.info.id, {id => value})
      end
    end

    # 加入時移譲
    def transfer(predecessor)
      return if @dhash.hash_table.count == 0

      # 自分が保持している key&value のうち新しい predecessor が担当となるものを委譲
      entries = {}
      @dhash.hash_table.each do |key, value|
        if Util.betweenE(key, @chord.info.id, predecessor.id)
          entries.store(key, value)
          @dhash.hash_table.delete(key)
        end
      end
      @replicas.store(predecessor.id, entries)
      DRbObject::new_with_uri(predecessor.uri("dhash")).insert_entries(entries)

      # successor_list の最後のノードのレプリカから、predecessor のものを削除
      if @chord.successor_list.count == SLIST_SIZE
        last_successor = @chord.successor_list.last
        begin
          last_successor_dhash = DRbObject::new_with_uri(last_successor.uri("dhash"))
          last_successor_dhash.replication.delete(predecessor.id)
        rescue DRb::DRbConnError
        end
      end

      # 新しい replica の配置
      @chord.successor_list.each do |s|
        begin
          successor_dhash = DRbObject::new_with_uri(s.uri("dhash"))
          successor_dhash.replication.insert(@chord.info.id, @dhash.hash_table)
        rescue DRb::DRbConnError
        end
      end
    end

    # 自動再 put
    def reput
    end
  end
end
