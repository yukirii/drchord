#!/usr/bin/env ruby
# encoding: utf-8

require 'zlib'
require 'drb/drb'
require 'logger'

module DRChord
  class Node
    M = 32
    SLIST_SIZE = 3

    def initialize(options, logger = nil)
      @ip = options[:ip]
      @port = options[:port]

      @logger = logger || Logger.new(STDERR)

      @finger = []
      @successor_list = []
      @predecessor = nil

      @hash_table = {}
      @replicas = {}

      @next = 0
      @active = false
    end
    attr_accessor :ip, :port, :finger, :successor_list, :predecessor
    attr_reader :logger, :hash_table, :replicas

    def active?
      return @active
    end

    def successor
      return @finger[0]
    end

    def successor=(node)
      @finger[0] = node
      logger.info "set successor = #{@finger[0]}"
    end

    def info
      return {:ip => @ip, :port => @port, :id => id, :uri => "druby://#{@ip}:#{@port}"}
    end

    def id
      return Zlib.crc32("#{@ip}:#{@port}")
    end

    def key_transfer(node)
      entries = {}
      # 譲渡するエントリを自身のhash_tableから削除
      @hash_table.each do |key, value|
        if betweenE(key, @predecessor[:id], node[:id])
          entries.store(key, value)
          @hash_table.delete(key)
        end
      end
      # 譲渡するエントリを自身のreplicaとして登録
      @replicas.store(node[:id], entries)

      return entries
    end

    def join(bootstrap_node = nil)
      if bootstrap_node.nil?
        @predecessor = nil
        self.successor = self.info
        (M-1).times { @finger << self.info }
      else
        @predecessor = nil
        begin
          node = DRbObject::new_with_uri(bootstrap_node)
          self.successor = node.find_successor(self.id)
        rescue DRb::DRbConnError => ex
          logger.error "Error: Connection failed - #{node.__drburi}"
          logger.error ex.message
          exit
        end
        build_finger_table(bootstrap_node)
        succ = DRbObject::new_with_uri(self.successor[:uri])
        @hash_table.merge!(succ.key_transfer(self.info))
      end

      @successor_list = []
      @successor_list << @finger[0]
      while @successor_list.count < SLIST_SIZE
        if bootstrap_node.nil?
          @successor_list << self.info
        else
          last_node = DRbObject::new_with_uri(@successor_list.last[:uri])
          @successor_list << last_node.successor
        end
      end

      @active = true
    end

    def build_finger_table(bootstrap_node)
      node = DRbObject::new_with_uri(bootstrap_node)
      0.upto(M-2) do |i|
        if Ebetween(finger_start(i+1), self.id,  @finger[i][:id])
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
        pred_node = DRbObject::new_with_uri(pred[:uri])
        pred_node.update_finger_table(self.info, i)
      end
    end

    def update_finger_table(s, i)
      if self.id != s[:id] && Ebetween(s[:id], self.id, @finger[i][:id])
        @finger[i] = s
        pred_node = DRbObject::new_with_uri(@predecessor[:uri])
        pred_node.update_finger_table(s, i)
      end
    end

    def notify(n)
      if @predecessor == nil || between(n[:id], @predecessor[:id], self.id)
        @predecessor = n
        logger.info "set predecessor = #{n}"
      end
    end

    def find_successor(id)
      if betweenE(id, self.id, self.successor[:id])
        return self.successor
      else
        n1 = self.closest_preceding_finger(id)
        node = DRbObject::new_with_uri(n1[:uri])
        return node.find_successor(id)
      end
    end

    def find_predecessor(id)
      return @predecessor if id == self.id

      n1 = DRbObject::new_with_uri(self.info[:uri])
      while betweenE(id, n1.id, n1.successor[:id]) == false
        n1_info= n1.closest_preceding_finger(id)
        n1 = DRbObject::new_with_uri(n1_info[:uri])
      end
      return n1.info
    end

    def closest_preceding_finger(id)
      (M-1).downto(0) do |i|
        if between(@finger[i][:id], self.id, id)
          return @finger[i] if alive?(@finger[i][:uri])
        end
      end
      return self.info
    end

    def stabilize
      return if active? == false

      # 現在の successor が生きているか調べる
      if self.successor != nil && alive?(self.successor[:uri]) == false
        logger.info "Stabilize: Successor node failure has occurred."

        @successor_list.delete_at(0)
        if @successor_list.count == 0
          (M-1).downto(0) do |i|
            if alive?(@finger[i][:uri]) == true
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
      succ_node = DRbObject::new_with_uri(self.successor[:uri])
      x = succ_node.predecessor
      if x != nil && alive?(x[:uri])
        if between(x[:id], self.id, self.successor[:id])
          self.successor = x
        end
      end
      succ_node.notify(self.info)
    end

    def fix_fingers
      @next += 1
      @next = 0 if @next >= M
      @finger[@next] = find_successor(finger_start(@next))
    end

    def fix_successor_list
      list = DRbObject::new_with_uri(self.successor[:uri]).successor_list
      list.unshift(self.successor)
      @successor_list = list[0..SLIST_SIZE-1]
    end

    def fix_predecessor
      if @predecessor != nil && alive?(@predecessor[:uri]) == false
        @predecessor = nil
        logger.info "fix_predecessor: Predecessor node failure has occurred.  set predecessor = nil"
      end
    end

    def notify_predecessor_leaving(node, new_pred, pred_hash)
      if node == @predecessor
        @predecessor = new_pred
        @hash_table.merge!(pred_hash)
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
      logger.info "Node #{self.info[:uri]} leaving..."
      DRbObject::new_with_uri(self.successor[:uri]).notify_predecessor_leaving(self.info, @predecessor, @hash_table)
      DRbObject::new_with_uri(@predecessor[:uri]).notify_successor_leaving(self.info, @successor_list)
      @active = false
    end

    def get(key)
      return false if key == nil

      id = Zlib.crc32(key)
      succ = find_successor(id)
      if succ == self.info
        logger.info "#{self.info[:uri]}: get key:#{key}"

        ret = @hash_table.fetch(id, nil)
        if ret.nil?
          # hash_table にない場合 replica 内を探す
          @replicas.each do |node_id, hash|
            if id <= node_id
              ret = hash.fetch(id, nil)
              break unless ret.nil?
            end
          end
        end
        return ret
      else
        return DRbObject::new_with_uri(succ[:uri]).get(key)
      end
    end

    def put(key, value)
      return false if key == nil

      id = Zlib.crc32(key)
      succ = find_successor(id)
      if succ == self.info
        @hash_table.store(id, value)
        logger.info "#{self.info[:uri]}: put key:#{key} value:#{value}"
        @successor_list.each do |s|
          DRbObject::new_with_uri(s[:uri]).insert_replicas(self.id, @hash_table)
        end
        return true
      else
        DRbObject::new_with_uri(succ[:uri]).put(key, value)
      end
    end

    def insert_replicas(node_id, entries)
      @replicas.store(node_id, entries)
    end

    def delete(key)
      return false if key == nil

      id = Zlib.crc32(key)
      succ = find_successor(id)
      if succ == self.info
        @hash_table.delete(id)
        @successor_list.each do |s|
          DRbObject::new_with_uri(s[:uri]).delete_replica(self.id, id)
        end
        logger.info "#{self.info[:uri]}: delete key:#{key}"
        return true
      else
        DRbObject::new_with_uri(succ[:uri]).delete(key)
      end
    end

    def delete_replica(node_id, replica = nil)
      if replicas.nil?
        @replicas.reject!{|key, value| key == node_id }
      else
        @replicas[node_id].reject!{|key, value| value == replica }
      end
    end

    def transfer_replicas
      @successor_list.each do |s|
        DRbObject::new_with_uri(s[:uri]).insert_replicas(self.id, @hash_table)
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
