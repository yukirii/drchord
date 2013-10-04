#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require 'zlib'
require 'drb/drb'
require 'logger'

module DRChord
  class Node
    M = 32
    SLIST_SIZE = 3
    INTERVAL = 5

    def initialize(options, logger = nil)
      @logger = logger || Logger.new(STDERR)

      @info = NodeInformation.new(options[:ip], options[:port])

      @finger = []
      @successor_list = []
      @predecessor = nil

      @hash_table = {}
      @replicas = {}

      @next = 0
      @active = false
    end
    attr_reader :logger, :info, :finger, :successor_list, :hash_table, :replicas, :predecessor

    def active?
      return @active
    end

    def successor
      return @finger[0]
    end

    def successor=(node)
      @finger[0] = node
      logger.info "set successor = #{@finger[0].uri}"
    end

    def predecessor=(node)
      @predecessor = node
      logger.info "set predecessor = #{node.nil? ? "nil" : node.uri}"

      if node != nil && node != @info
        entries = {}
        # 譲渡するエントリを自身のhash_tableから削除
        @hash_table.each do |key, value|
          if betweenE(key, self.id, @predecessor.id)
            entries.store(key, value)
            @hash_table.delete(key)
          end
        end
        @replicas.store(node.id, entries)
        DRbObject::new_with_uri(@predecessor.uri).insert_entries(entries)

        # successor_list の最後のノードのreplicaのうち、@predecessorのものを削除
        if @successor_list.count == SLIST_SIZE
          last_successor = @successor_list.last
          begin
            DRbObject::new_with_uri(last_successor.uri).delete_replica(self.id, @predecessor.id)
          rescue DRb::DRbConnError
          end
        end

        # 新しい replica の配置
        @successor_list.each do |s|
          begin
            DRbObject::new_with_uri(s.uri).insert_replicas(self.id, @hash_table)
          rescue DRb::ConnError
          end
        end
      end
    end

    def id
      return @info.id
      #return Zlib.crc32("#{@ip}:#{@port}")
    end

    def insert_entries(entries)
      @hash_table.merge!(entries)
    end

    def join(bootstrap_node = nil)
      if bootstrap_node.nil?
        self.predecessor = nil
        self.successor = @info
        (M-1).times { @finger << @info }
      else
        self.predecessor = nil
        begin
          node = DRbObject::new_with_uri(bootstrap_node)
          self.successor = node.find_successor(self.id)
        rescue DRb::DRbConnError => ex
          logger.error "Error: Connection failed - #{node.__drburi}"
          logger.error ex.message
          exit
        end
        build_finger_table(bootstrap_node)
      end

      @successor_list = []
      @successor_list << @finger[0]
      while @successor_list.count < SLIST_SIZE
        if bootstrap_node.nil?
          @successor_list << @info
        else
          last_node = DRbObject::new_with_uri(@successor_list.last.uri)
          @successor_list << last_node.successor
        end
      end

      @active = true
    end

    def build_finger_table(bootstrap_node)
      node = DRbObject::new_with_uri(bootstrap_node)
      0.upto(M-2) do |i|
        if Ebetween(finger_start(i+1), self.id,  @finger[i].id)
          @finger[i+1] = @finger[i]
        else
          begin
            @finger[i+1] = node.find_successor(finger_start(i+1))
          rescue DRb::DRbConnError => ex
            logger.error "Error: Connection failed - #{node.__drburi}"
            logger.error ex.message
            exit
          end
        end
      end
    end

    def update_others
      0.upto(M-1) do |i|
        pred = find_predecessor(self.id - 2**i)
        pred_node = DRbObject::new_with_uri(pred.uri)
        pred_node.update_finger_table(@info, i)
      end
    end

    def update_finger_table(s, i)
      if self.id != s.id && Ebetween(s.id, self.id, @finger[i].id)
        @finger[i] = s
        pred_node = DRbObject::new_with_uri(@predecessor.uri)
        pred_node.update_finger_table(s, i)
      end
    end

    def notify(n)
      if @predecessor == nil || between(n.id, @predecessor.id, self.id)
        self.predecessor = n
      end
    end

    def find_successor(id)
      if betweenE(id, self.id, self.successor.id)
        return self.successor
      else
        n1 = self.closest_preceding_finger(id)
        node = DRbObject::new_with_uri(n1.uri)
        return node.find_successor(id)
      end
    end

    def find_predecessor(id)
      return @predecessor if id == self.id

      n1 = DRbObject::new_with_uri(@info.uri)
      while betweenE(id, n1.id, n1.successor.id) == false
        n1_info= n1.closest_preceding_finger(id)
        n1 = DRbObject::new_with_uri(n1_info.uri)
      end
      return n1.info
    end

    def closest_preceding_finger(id)
      (M-1).downto(0) do |i|
        if between(@finger[i].id, self.id, id)
          return @finger[i] if alive?(@finger[i].uri)
        end
      end
      return @info
    end

    def stabilize
      return if active? == false

      # 現在の successor が生きているか調べる
      if self.successor != nil && alive?(self.successor.uri) == false
        logger.info "Stabilize: Successor node failure has occurred."

        @successor_list.delete_at(0)
        if @successor_list.count == 0
          (M-1).downto(0) do |i|
            if alive?(@finger[i].uri) == true
              self.successor = @finger[i]
              stabilize
              return
            end
          end

          # There is nothing we can do, its over.
          @active = false
          return
        else
          self.successor = @successor_list.first
          stabilize
          return
        end
      end

      # successor の predecessor を取得
      succ_node = DRbObject::new_with_uri(self.successor.uri)
      x = succ_node.predecessor
      if x != nil && alive?(x.uri)
        if between(x.id, self.id, self.successor.id)
          self.successor = x
        end
      end
      succ_node.notify(@info)
    end

    def fix_fingers
      @next += 1
      @next = 0 if @next >= M
      @finger[@next] = find_successor(finger_start(@next))
    end

    def fix_successor_list
      list = DRbObject::new_with_uri(self.successor.uri).successor_list
      list.unshift(self.successor)
      @successor_list = list[0..SLIST_SIZE-1]
    end

    def fix_predecessor
      if @predecessor != nil && alive?(@predecessor.uri) == false
        old_predecessor = @predecessor
        self.predecessor = nil

        # predecessor のレプリカを自身の hash_table にマージし、レプリカからは削除する
        @hash_table.merge!(@replicas[old_predecessor.id])
        @replicas.delete(old_predecessor.id)
      end
    end

    def notify_predecessor_leaving(node, new_pred, pred_hash)
      if node == @predecessor
        old_pred = @predecessor
        self.predecessor = new_pred
        @hash_table.merge!(pred_hash)
        delete_replica(old_pred.id)
      end
    end

    def notify_successor_leaving(node, successors)
      if node == self.successor
        @successor_list.delete_at(0)
        @successor_list << successors.last
        self.successor = @successor_list.first
      end
    end

    def leave
      logger.info "Node #{@info.uri} leaving..."
      if self.successor != @predecessor
        begin
          DRbObject::new_with_uri(self.successor.uri).notify_predecessor_leaving(@info, @predecessor, @hash_table)
          DRbObject::new_with_uri(@predecessor.uri).notify_successor_leaving(@info, @successor_list) if @predecessor != nil
        rescue DRb::DRbConnError
        end
      end
      @active = false
    end

=begin
    def get(key)
      return false if key == nil

      id = Zlib.crc32(key)
      succ = find_successor(id)
      if succ == @info
        logger.info "#{self.info.uri}: get key:#{key}"

        ret = @hash_table.fetch(id, nil)
        if ret.nil?
          # hash_table にない場合 replica 内を探す
          @replicas.each do |node_id, hash|
            ret = hash.fetch(id, nil)
            break unless ret.nil?
          end
        end
        return ret
      else
        return DRbObject::new_with_uri(succ.uri).get(key)
      end
    end

    def put(key, value)
      return false if key == nil

      id = Zlib.crc32(key)
      succ = find_successor(id)
      if succ == self.info
        @hash_table.store(id, value)
        logger.info "#{@info.uri}: put key:#{key} value:#{value}"
        @successor_list.each do |s|
          DRbObject::new_with_uri(s.uri).insert_replicas(self.id, @hash_table)
        end
        return true
      else
        DRbObject::new_with_uri(succ.uri).put(key, value)
      end
    end

    def delete(key)
      return false if key == nil

      id = Zlib.crc32(key)
      succ = find_successor(id)
      if succ == @info
        ret = @hash_table.delete(id)
        unless ret.nil?
          @successor_list.each do |s|
            DRbObject::new_with_uri(s.uri).delete_replica(self.id, id)
          end
          logger.info "#{@info.uri}: delete key:#{key}"
        end
        return ret
      else
        DRbObject::new_with_uri(succ.uri).delete(key)
      end
    end
=end

    def insert_replicas(node_id, entries)
      # 自分自身のレプリカは持たない
      if self.id != node_id
        @replicas.store(node_id, entries)
      end
    end

    def delete_replica(node_id, replica = nil)
      if replica.nil?
        @replicas.reject!{|key, value| key == node_id }
      else
        if @replicas[node_id].nil? == false
          @replicas[node_id].reject!{|key, value| betweenE(key, node_id, replica) }
        end
      end
    end

    def management_replicas
      # successor == predecessor (Ringに自ノードのみ)の場合は全レプリカを hash_table に移動
      if @predecessor == @info && self.successor == @predecessor
        if @replicas.count > 0
          @replicas.each{|key, value| @hash_table.merge!(value) }
          @replicas.clear
        end
      end

      # successor_list に最新の replica を配置
      @successor_list.each do |s|
        begin
          DRbObject::new_with_uri(s.uri).insert_replicas(self.id, @hash_table)
        rescue DRb::DRbConnError
        end
      end
    end

    def start(bootstrap_node)
      logger.info "Ctrl-C to shutdown node"
      join(bootstrap_node)
      begin
        loop do
          if active? == true
            stabilize
            fix_fingers
            fix_successor_list
            fix_predecessor
            management_replicas
          end
          sleep INTERVAL
        end
      rescue Interrupt
        logger.info "going to shutdown..."
        leave
      end
    end

    private
    def alive?(uri)
      begin
        node = DRbObject::new_with_uri(uri)
        return node.active?
      rescue DRb::DRbConnError
        return false
      end
    end

    def finger_start(k)
      return (self.id + 2**k) % 2**M
    end

    def between(value, initv, endv)
      return true if initv == endv && initv != value && endv != value
      if initv < endv
        return true if initv < value && value < endv
      else
        return true if value < 0
        return true if ((initv < value && value < 2**M) || (0 <= value && value < endv))
      end
      return false
    end

    def Ebetween(value, initv, endv)
      return value == initv ? true : between(value, initv, endv)
    end

    def betweenE(value, initv, endv)
      return value == endv ? true : between(value, initv, endv)
    end
  end
end
