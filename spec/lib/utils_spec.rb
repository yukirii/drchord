# encoding: utf-8

require './lib/utils.rb'
require 'spec_helper'

describe DRChord::Utils do
  before do
    @hash_bit = DRChord::HASH_BIT
    @max_num = 2**@hash_bit - 1
  end

  describe "between" do
    it "initv と endv が等しい場合 true が返される" do
      expect(DRChord::Utils.between(rand(@max_num), 0, 0)).to be_truthy
    end

    it "引数がすべて等しい場合 false が返される" do
      expect(DRChord::Utils.between(0, 0, 0)).to be_falsey
      expect(DRChord::Utils.between(@max_num, @max_num, @max_num)).to be_falsey
    end

    it "initv < value < endv で true が返される" do
      expect(DRChord::Utils.between(1000, 100, 10000)).to be_truthy
    end

    it "value == initv または value == endv のとき false が返される" do
      expect(DRChord::Utils.between(1000, 1000, 10000)).to be_falsey
      expect(DRChord::Utils.between(10000, 1000, 10000)).to be_falsey
    end

    context "value が負の値の時" do
      it "initv > endv のとき true が返される" do
        expect(DRChord::Utils.between(-1, 100, 10)).to be_truthy
      end

      it "initv < endv のとき false が返される" do
        expect(DRChord::Utils.between(-1, 10, 100)).to be_falsey
      end
    end
  end

  describe "betweenE" do
    it "initv と endv が等しい場合 true が返される" do
      expect(DRChord::Utils.betweenE(rand(@max_num), 0, 0)).to be_truthy
    end

    it "ID 空間上で initv < value <= endv の場合 true が返される" do
      expect(DRChord::Utils.betweenE(50, 10, 100)).to be_truthy
      expect(DRChord::Utils.betweenE(100, 10, 100)).to be_truthy
    end

    it "ID 空間上で value <= initv で false が返される" do
      expect(DRChord::Utils.betweenE(0, 10, 100)).to be_falsey
      expect(DRChord::Utils.betweenE(10, 10, 100)).to be_falsey
    end

    context "value が負の値の時" do
      it "initv > endv のとき true が返される" do
        expect(DRChord::Utils.betweenE(-1, 100, 10)).to be_truthy
      end

      it "initv < endv のとき false が返される" do
        expect(DRChord::Utils.betweenE(-1, 10, 100)).to be_falsey
      end
    end
  end

  describe "Ebetween" do
    it "initv と endv が等しい場合 true が返される" do
      expect(DRChord::Utils.Ebetween(rand(@max_num), 0, 0)).to be_truthy
    end

    it "ID 空間上で initv <= value < endv の場合 true が返される" do
      expect(DRChord::Utils.Ebetween(10, 10, 100)).to be_truthy
      expect(DRChord::Utils.Ebetween(50, 10, 100)).to be_truthy
    end

    it "ID 空間上で value >= endv で false が返される" do
      expect(DRChord::Utils.Ebetween(100, 10, 100)).to be_falsey
      expect(DRChord::Utils.Ebetween(150, 10, 100)).to be_falsey
    end

    context "value が負の値の時" do
      it "initv > endv のとき true が返される" do
        expect(DRChord::Utils.Ebetween(-1, 100, 10)).to be_truthy
      end

      it "initv < endv のとき false が返される" do
        expect(DRChord::Utils.Ebetween(-1, 10, 100)).to be_falsey
      end
    end
  end
end

