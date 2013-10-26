# encoding: utf-8

require './lib/front.rb'
require './lib/chord.rb'
require './lib/node_info.rb'
require 'spec_helper'
require 'drb/drb'

describe DRChord::DHash do
  before :all do
    @front = DRChord::Front.new(nil)
    @dhash = @front.dhash
    @front.start
  end
end
