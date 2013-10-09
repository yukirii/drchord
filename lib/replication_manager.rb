#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))
require  File.expand_path(File.join(drchord_dir, '/node_info.rb'))

module DRChord
  class ReplicationManager
    INTERVAL = 3

    def initialize(chord)
      @chord = chord
    end

    def start
      @replica_thread = Thread.new do
        loop do
          if @chord.active?
            @chord.successor_list
          end
          sleep INTERVAL
        end
      end
    end

    def stop
      @replica_thread.kill
    end

    def create_replica
    end

    def transfer
    end

    def automatic_re_put
    end
  end
end
