require 'spec_helper'

describe DCell::Registry::RiakAdapter do
  subject { DCell::Registry::RiakAdapter.new :env => 'test', http_port: ENV['RIAK_HTTP_PORT'] || 8098 }
  it_behaves_like "a DCell registry"
end
