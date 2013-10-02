#!/usr/bin/env ruby
# encoding: utf-8

require "zlib"

drchord_dir = File.expand_path(File.dirname(__FILE__))
require  File.expand_path(File.join(drchord_dir, '/node.rb'))

module DRChord
  class DHash
    attr_reader :node
    def initialize(chord, logger)
      @chord = chord
      @hash_table = {}

      @logger = logger || Logger.new(STDERR)
    end

    def start(bootstrap)
      @chord.start(bootstrap)
    end

    def put(key, value)
      "DRChord::Dhash#put"
    end

    def get(key)
      "DRChord::Dhash#get"
    end

    def delete(key)
      "DRChord::Dhash#delete"
    end
  end
end
