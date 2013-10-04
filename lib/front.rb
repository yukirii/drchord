#!/usr/bin/env ruby
# encoding: utf-8

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))
require  File.expand_path(File.join(drchord_dir, '/dhash.rb'))

module DRChord
  class Front
    attr_reader :chord, :dhash
    def initialize(options, logger)
      @chord = Node.new(options, logger)
      @dhash = DHash.new(@chord, logger)
      @active = true
    end

    def [](args)
      case args
      when "chord"; return @chord
      when "dhash"; return @dhash
      end
    end

    def active?
      return @active
    end
  end
end
