#!/usr/bin/env ruby
# encoding: utf-8

require "zlib"

module DRChord
  class DHash
    attr_reader :node
    def initialize(chord, logger)
      @chord = chord
      @hash_table = {}

      @logger = logger || Logger.new(STDERR)
    end

    def start(bootstrap = nil)
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
