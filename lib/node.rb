#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))
require  File.expand_path(File.join(drchord_dir, '/util.rb'))
require 'observer'
require 'drb/drb'
require 'logger'
require 'zlib'

module DRChord
  class Node
    include Observable

    M = 32
    SLIST_SIZE = 3
    INTERVAL = 5

    def initialize(options, logger = nil)
      @logger = logger || Logger.new(STDERR)

      @info = NodeInformation.new(options[:ip], options[:port])

      @finger = []
      @successor_list = []
      @predecessor = nil

      #@hash_table = {}

      @next = 0
      @active = false
    end
    attr_reader :logger, :info, :finger, :successor_list, :hash_table, :replicas, :predecessor

    def id
      return @info.id
    end

    def active?
      return @active
    end

    def successor
      return @finger[0]
    end

    def successor=(node)
      @finger[0] = node
      logger.debug "set successor = #{@finger[0].uri}"
    end

    def predecessor=(node)
      @predecessor = node
      logger.debug "set predecessor = #{node.nil? ? "nil" : node.uri}"

      if node != nil && node != @info
        changed
        notify_observers(@predecessor)
      end
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
          logger.error "Connection failed - #{node.__drburi}"
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
        if Util.Ebetween(finger_start(i+1), self.id,  @finger[i].id)
          @finger[i+1] = @finger[i]
        else
          begin
            @finger[i+1] = node.find_successor(finger_start(i+1))
          rescue DRb::DRbConnError => ex
            logger.error "Connection failed - #{node.__drburi}"
            logger.error ex.message
            exit
          end
        end
      end
    end

    def update_finger_table(s, i)
      if self.id != s.id && Util.Ebetween(s.id, self.id, @finger[i].id)
        @finger[i] = s
        pred_node = DRbObject::new_with_uri(@predecessor.uri)
        pred_node.update_finger_table(s, i)
      end
    end

    def notify(n)
      if @predecessor == nil || Util.between(n.id, @predecessor.id, self.id)
        self.predecessor = n
      end
    end

    def find_successor(id)
      if Util.betweenE(id, self.id, self.successor.id)
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
      while Util.betweenE(id, n1.id, n1.successor.id) == false
        n1_info= n1.closest_preceding_finger(id)
        n1 = DRbObject::new_with_uri(n1_info.uri)
      end
      return n1.info
    end

    def closest_preceding_finger(id)
      (M-1).downto(0) do |i|
        if Util.between(@finger[i].id, self.id, id)
          return @finger[i] if alive?(@finger[i].uri)
        end
      end
      return @info
    end

    def start(bootstrap_node)
      join(bootstrap_node)
      @chord_thread = Thread.new do
        loop do
          if active? == true
            stabilize
            fix_fingers
            fix_successor_list
            fix_predecessor
            #management_replicas
          end
          sleep INTERVAL
        end
      end
    end

    def leave
      logger.info "Node #{@info.uri} leaving..."
      @chord_thread.kill
      if self.successor != @predecessor
        begin
          DRbObject::new_with_uri(self.successor.uri).notify_predecessor_leaving(@info, @predecessor, @hash_table)
          DRbObject::new_with_uri(@predecessor.uri).notify_successor_leaving(@info, @successor_list) if @predecessor != nil
        rescue DRb::DRbConnError
        end
      end
      @active = false
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

    def successor_candidates(id, max_number)
      candidates_list = []
      first_node = DRbObject::new_with_uri(find_successor(id).uri)
      candidates_list << first_node.successor
      while candidates_list.count < max_number
        last_node = DRbObject::new_with_uri(candidates_list.last.uri)
        candidates_list << last_node.successor
      end
      return candidates_list
    end

=begin
    def insert_entries(entries)
      @hash_table.merge!(entries)
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
=end

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

    def stabilize
      return if active? == false

      # 現在の successor が生きているか調べる
      if self.successor != nil && alive?(self.successor.uri) == false
        logger.debug "Stabilize: Successor node failure has occurred."

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
        if Util.between(x.id, self.id, self.successor.id)
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
        #@hash_table.merge!(@replicas[old_predecessor.id])
        #@replicas.delete(old_predecessor.id)
      end
    end
  end
end
