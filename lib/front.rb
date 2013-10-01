#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))

module DRChord
  class Front
    attr_reader :node
    def initialize(options, logger)
      @node = DRChord::Node.new(options, logger)
    end

    def [](args)
      case args
      when "node"
        @node
      end
    end
  end
end
