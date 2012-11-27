require 'spec_helper'

describe DCell::Registry::RiakAdapter do
  subject { DCell::Registry::RiakAdapter.new :env => 'test' }
  it_behaves_like "a DCell registry"
end
