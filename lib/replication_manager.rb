#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '/dhash.rb'))
require  File.expand_path(File.join(drchord_dir, '/util.rb'))

module DRChord
  class ReplicationManager
    INTERVAL = 30
    SLIST_SIZE = 3
    NUMBER_OF_COPIES = 3

    def initialize(dhash)
      @dhash = dhash
      @chord = @dhash.chord
      @chord.add_observer(self, :transfer)
    end

    def start
      @reput_thread = Thread.new do
        loop do
          reput
          sleep INTERVAL + rand(30)
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
        dhash = DRbObject::new_with_uri(s.uri("dhash"))
        dhash.hash_table = dhash.hash_table.merge({id => value})
      end
    end

    def delete(id)
      candidates_list = @chord.successor_candidates(id, NUMBER_OF_COPIES)
      candidates_list.each do |n|
        dhash = DRbObject::new_with_uri(n.uri("dhash"))
        dhash.hash_table = dhash.hash_table.reject{|key, value| key == id }
      end
    end

    # 加入時移譲
    def transfer(predecessor)
      candidates_list = @chord.successor_candidates(@chord.id, NUMBER_OF_COPIES)

      pair = {}
      candidates_list.each do |n|
        node = DRbObject::new_with_uri(n.uri("dhash"))
        kv_pair = node.request_kv_pair(@chord.id)
        pair = pair.merge(kv_pair)
      end
      @dhash.hash_table = @dhash.hash_table.merge(pair)
    end

    private
    # 自動再 put
    def reput
      if @chord.active?
        @dhash.hash_table.each do |key, value|
          @dhash.put(key, value, false)
        end
      end
    end
  end
end
