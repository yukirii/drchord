#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/chord.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '/replicator.rb'))
require "monitor"
require "zlib"

module DRChord
  # ハッシュテーブルの基本機能を提供する
  class DHash
    include MonitorMixin

    attr_reader :logger, :chord, :replicator
    attr_accessor :hash_table
    def initialize(chord, logger)
      @logger = logger || Logger.new(STDERR)
      @chord = chord
      @replicator = Replicator.new(self, logger)
      @monitor = Monitor.new
      @hash_table = {}
    end

    # DHT ノードを立ち上げる
    # @param [Object] bootstrap DHT に既に参加しているノードの接続情報
    def start(bootstrap = nil)
      @chord_thread = @chord.start(bootstrap)
      @replicator_thread = @replicator.start
      @chord_thread.join
      @replicator_thread.join
    end

    # DHT ノードを終了する
    def shutdown
      logger.info "going to shutdown..."
      @replicator.stop
      @chord.leave
    end

    # DHT に Key-Value を保存する
    # @param [String] key 保存したい Value に対応付ける Key
    # @param [String] value 保存したい Value
    # @param [Boolean] calculate_hash Key のハッシュ値を計算する場合 true, しない場合 false
    # @return [Boolean] Key-Value の保存に成功した場合 true, 失敗した場合 false
    def put(key, value, calculate_hash = true)
      return false if key == nil

      id = calculate_hash ? Zlib.crc32(key) : key
      successor_node = @chord.find_successor(id)
      if successor_node.id == @chord.info.id
        @monitor.synchronize do
          @hash_table.store(id, value)
        end
        logger.debug "#{@chord.info.uri("dhash")}: stored key:#{key}"

        unless @chord.is_alone?
          @replicator.create(id, value)
        end
        return true
      else
        begin
          return DRbObject::new_with_uri(successor_node.uri("dhash")).put(key, value, calculate_hash)
        rescue DRb::DRbConnError
          return false
        end
      end
    end

    # DHT に保存されている Value を取得する
    # @param [String] key 取得したい Value に対応付けた Key
    # @return [String] Key に対応付けられた Value
    # @return [Boolean] Value の取得に失敗した場合 false
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

    # DHT に保存されている Value を取得する
    #
    # 他のノードへのリクエスト転送は行わず、自ノードの hash_table のみを探索する
    # @param [String] key 取得したい Value に対応付けた Key
    # @return [String] Key に対応付けられた Value
    # @return [Boolean] Value の取得に失敗した場合 false
    def get_local(key)
      id = Zlib.crc32(key)
      ret = @hash_table.fetch(id, nil)
      return ret != nil && ret != false ? ret : false
    end

    # DHT に保存されている Key-Value を削除する
    # @param [String] key 削除したい Value に対応付けた Key
    # @return [Boolean] 削除に成功した場合 true, 失敗した場合 false
    def delete(key)
      return false if key == nil

      id = Zlib.crc32(key)
      successor_node = @chord.find_successor(id)
      if successor_node.id == @chord.info.id
        ret = @hash_table.delete(id)
        unless ret.nil?
          @replicator.delete(id)
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

    # key の root (successor) の候補一覧を返す
    # @param [String] key Value に対応付けた Key
    # @return [Array] 候補ノード情報が要素となる Array
    def lookup_roots(key)
      id = Zlib.crc32(key)
      candidates_list = @chord.successor_candidates(id, 3)
      candidates_list = candidates_list.uniq.map{|x| x.uri("dhash") }
      return candidates_list
    end

    private
    # Key を持っている候補のリストに get_local リクエストを行う
    # @return [String] Key に対応付けられた Value
    # @return [Boolean] Value の取得に失敗した場合 false
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
