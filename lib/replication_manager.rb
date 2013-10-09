#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '/dhash.rb'))

module DRChord
  class ReplicationManager
    INTERVAL = 10

    attr_reader :replicas
    def initialize(chord, dhash)
      @chord = chord
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

    # 新規レプリカの配置処理
    def create(id, value, successor_list)
      successor_list.each do |s|
        dhash = DRbObject::new_with_uri(s.uri("dhash"))
        dhash.replication.insert(@chord.info.id, id, value)
      end
    end

    def insert(node_id, id, value)
      return if @chord.info.id == node_id
      if @replicas[node_id].nil?
        @replicas[node_id] = {id => value}
      else
        @replicas[node_id].store(id, value)
      end
    end

    # 加入時移譲
    def transfer
      p "DRChord::ReplicationManager#transfer"
    end

    # 自動再 put
    def reput
    end
  end
end
