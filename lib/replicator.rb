#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/chord.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '/dhash.rb'))
require  File.expand_path(File.join(drchord_dir, '/util.rb'))

module DRChord
  # Key-Value のレプリカの管理を行う
  class Replicator
    attr_reader :logger
    def initialize(dhash, logger)
      @logger = logger || Logger.new(STDERR)
      @dhash = dhash
      @chord = @dhash.chord
      @chord.add_observer(self, :transfer)
    end

    # 自動再 put を行うスレッドを開始する
    def start
      @reput_thread = Thread.new do
        loop do
          sleep DRChord::AUTO_REPUT_INTERVAL - 5 + rand(10)
          unless @chord.is_alone?
            reput
          end
        end
      end
    end

    # 自動再 put を行うスレッドを停止する
    def stop
      @reput_thread.kill
    end

    # レプリカの新規作成・配置処理を行う
    # @param [Fixnum] id Key (String) のハッシュ値
    # @param [String] value Key に対応する Value
    def create(id, value)
      Thread.new do
        candidates_list = @chord.successor_candidates(id, DRChord::NUMBER_OF_COPIES)
        candidates_list.each do |s|
          if s.id != @chord.id
            dhash = DRbObject::new_with_uri(s.uri("dhash"))
            dhash.hash_table = dhash.hash_table.merge({id => value})
          end
        end
      end
    end

    # レプリカを削除する
    # @param [Fixnum] id Key (String) のハッシュ値
    def delete(id)
      candidates_list = @chord.successor_candidates(id, DRChord::NUMBER_OF_COPIES)
      candidates_list.each do |s|
        if s.id != @chord.id
          dhash = DRbObject::new_with_uri(s.uri("dhash"))
          dhash.hash_table = dhash.hash_table.reject{|key, value| key == id }
        end
      end
    end

    # 加入時移譲処理を行う
    #
    # DHT に既に存在する Key-Value のうち、新規加入した自ノードが新たな担当となる場合に Key-Value の委譲を受ける
    def transfer
      cnt = 0
      while cnt < 3
        candidates_list = @chord.successor_candidates(@chord.id, DRChord::NUMBER_OF_COPIES)
        succs_pred = DRbObject::new_with_uri(@chord.successor.uri).predecessor

        if succs_pred.nil?
          cnt += 1
          logger.debug "Key-Value transfer - Successor's predecessor is nil. retrying...(#{cnt})"
          sleep 3
        else
          pair = {}
          candidates_list.each do |n|
            dhash = DRbObject::new_with_uri(n.uri("dhash"))
            kv_pair = dhash.replicator.request_kv_pair(succs_pred.id, @chord.id)
            pair = pair.merge(kv_pair)
          end
          @dhash.hash_table = @dhash.hash_table.merge(pair)
          logger.debug "Key-Value transfer - successful. "
          return true
        end
      end
      logger.debug "Key-Value transfer - failed. "
    end

    # 新規加入ノードへ委譲する Key-Value を準備する
    # @param [Fixnum] pred 新規加入ノードの predecessor の ID
    # @param [Fixnum] node_id 新規加入ノードの ID
    # @return [Hash] 委譲する Key-Value ペアの格納された Hash
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
    # hash_table に持っている Key-Value ペアを DHT に再 put する
    def reput
      return false if @chord.active? == false

      @dhash.hash_table.each do |key, value|
        if keys_owner?(key) == false
          @dhash.hash_table = @dhash.hash_table.reject{|k, v| k == key }
          logger.debug "#{@chord.info.uri("dhash")}: Delete key:#{key}"
        else
          @dhash.put(key, value, false)
          logger.debug "#{@chord.info.uri("dhash")}: reput key:#{key}"
        end
      end
    end

    # Key-Value の担当もしくは担当候補ノードであるか調べる
    # @param [Fixnum] key 調べる Key-Value の Key
    # @return [boolean] 担当である場合 true, そうでない場合 false
    def keys_owner?(key)
      if @chord.predecessor != nil && Util::betweenE(key, @chord.predecessor.id, @chord.id) == true
        return true
      end

      candidates_list = @chord.successor_candidates(key, DRChord::NUMBER_OF_COPIES)
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
