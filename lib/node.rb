#!/usr/bin/env ruby
# encoding: utf-8

require 'zlib'
require 'drb/drb'

module DRChord
  class Node
    M = 32
    SLIST_SIZE = 3

    attr_accessor :ip, :port, :finger, :successor_list, :predecessor
    def initialize(options)
      options = default_options.merge(options)

      @ip = options[:ip]
      @port = options[:port]

      @finger = []
      @successor_list = []
      @predecessor = nil

      @active = false
    end

    def successor
      return @finger[0]
    end

    def successor=(node)
      @finger[0] = node
    end

    def info
      return {:ip => @ip, :port => @port, :id => id, :uri => "druby://#{@ip}:#{@port}"}
    end

    def id
      return Zlib.crc32("#{@ip}:#{@port}")
    end

    def join(n=nil)
      if n.nil?
        self.successor = self.info
        @predecessor = self.info
        (M-1).times { @finger << self.info }
      else
        init_finger_table(n)
        update_others()
      end

      @active = true

      @successor_list = []
      @successor_list << @finger[0]
      while @successor_list.count <= SLIST_SIZE
        if n.nil?
          @successor_list << self.info
        else
          last_node = DRbObject::new_with_uri(@successor_list.last[:uri])
          @successor_list << last_node.successor
        end
      end
    end

    def init_finger_table(n)
      node = DRbObject::new_with_uri("druby://#{n}")
      self.successor = node.find_successor(finger_start(0))

      succ_node = DRbObject::new_with_uri(self.successor[:uri])
      @predecessor = succ_node.predecessor
      succ_node.notify(self.info)

      0.upto(M-2) do |i|
        if Ebetween(finger_start(i+1), self.id,  @finger[i][:id])
          @finger[i+1] = @finger[i]
        else
          @finger[i+1] = node.find_successor(finger_start(i+1))
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
      if Ebetween(s[:id], self.id, @finger[i][:id]) && self.id != s[:id]
        @finger[i] = s
        pred_node = DRbObject::new_with_uri(@predecessor[:uri])
        pred_node.update_finger_table(s, i)
      end
    end

    def notify(n)
      if @predecessor == nil || between(n[:id], @predecessor[:id], self.id)
        @predecessor = n
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
          return @finger[i]
        end
      end
      return self.info
    end

    def stabilize
      # 現在の successor が生きているか調べる
      if self.successor != nil && alive?(self.successor[:uri]) == false
        @successor_list.delete_at(0)
        if @successor_list.count == 0
        else
          self.successor = @successor_list.first
          stabilize
          return
        end
      end

      # successor の predecessor を取得
      x = DRbObject::new_with_uri(self.successor[:uri]).predecessor
      if x != nil && alive?(x[:uri])
        if between(x[:id], self.id, self.successor[:id])
          self.successor = x
        end
      end
      DRbObject::new_with_uri(self.successor[:uri]).notify(self.info)

      # successor_list の更新
      fix_successor_list
    end

    def fix_fingers
      i = rand(M)
      @finger[i] = find_successor(finger_start(i))
    end

    def fix_successor_list
      list = DRbObject::new_with_uri(self.successor[:uri]).successor_list
      list.unshift(self.successor)
      @successor_list = list[0..SLIST_SIZE-1]
    end

    def fix_predecessor
      if @predecessor != nil && alive?(@predecessor[:uri]) == false
        @predecessor = nil
      end
    end

    def active?
      return @active
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

    def default_options
      return {:ip => '127.0.0.1', :port => 3000}
    end
  end
end
