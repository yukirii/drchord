# encoding: utf-8

require './lib/util.rb'
require 'spec_helper'

describe DRChord::Util do
  before do
    @hash_bit = DRChord::Util::M
    @max_num = 2**@hash_bit - 1
  end

  describe "between" do
    it "initv と endv が等しい場合 true が返される" do
      expect(DRChord::Util.between(rand(@max_num), 0, 0)).to be_true
    end

    it "引数がすべて等しい場合 false が返される" do
      expect(DRChord::Util.between(0, 0, 0)).to be_false
      expect(DRChord::Util.between(@max_num, @max_num, @max_num)).to be_false
    end

    it "initv < value < endv で true が返される" do
      expect(DRChord::Util.between(1000, 100, 10000)).to be_true
    end

    it "value == initv または value == endv のとき false が返される" do
      expect(DRChord::Util.between(1000, 1000, 10000)).to be_false
      expect(DRChord::Util.between(10000, 1000, 10000)).to be_false
    end

    context "value が負の値の時" do
      it "initv > endv のとき true が返される" do
        expect(DRChord::Util.between(-1, 100, 10)).to be_true
      end

      it "initv < endv のとき false が返される" do
        expect(DRChord::Util.between(-1, 10, 100)).to be_false
      end
    end
  end

  describe "betweenE" do
    it "initv と endv が等しい場合 true が返される" do
      expect(DRChord::Util.betweenE(rand(@max_num), 0, 0)).to be_true
    end

    it "ID 空間上で initv < value <= endv の場合 true が返される" do
      expect(DRChord::Util.betweenE(50, 10, 100)).to be_true
      expect(DRChord::Util.betweenE(100, 10, 100)).to be_true
    end

    it "ID 空間上で value <= initv で false が返される" do
      expect(DRChord::Util.betweenE(0, 10, 100)).to be_false
      expect(DRChord::Util.betweenE(10, 10, 100)).to be_false
    end

    context "value が負の値の時" do
      it "initv > endv のとき true が返される" do
        expect(DRChord::Util.betweenE(-1, 100, 10)).to be_true
      end

      it "initv < endv のとき false が返される" do
        expect(DRChord::Util.betweenE(-1, 10, 100)).to be_false
      end
    end
  end

  describe "Ebetween" do
    it "initv と endv が等しい場合 true が返される" do
      expect(DRChord::Util.Ebetween(rand(@max_num), 0, 0)).to be_true
    end

    it "ID 空間上で initv <= value < endv の場合 true が返される" do
      expect(DRChord::Util.Ebetween(10, 10, 100)).to be_true
      expect(DRChord::Util.Ebetween(50, 10, 100)).to be_true
    end

    it "ID 空間上で value >= endv で false が返される" do
      expect(DRChord::Util.Ebetween(100, 10, 100)).to be_false
      expect(DRChord::Util.Ebetween(150, 10, 100)).to be_false
    end

    context "value が負の値の時" do
      it "initv > endv のとき true が返される" do
        expect(DRChord::Util.Ebetween(-1, 100, 10)).to be_true
      end

      it "initv < endv のとき false が返される" do
        expect(DRChord::Util.Ebetween(-1, 10, 100)).to be_false
      end
    end
  end
end

