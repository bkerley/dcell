require 'riak'

module DCell
  module Registry
    class RiakAdapter
      def initialize(options)
        options = options.inject({}) { |h, (k,v)| h[k.to_sym] = v; h }

        @env = options[:env] || 'production'
        @prefix = options[:prefix] || "dcell_#{@env}_"

        options.delete :env
        options.delete :prefix

        @riak = Riak::Client.new options

        @node_registry = NodeRegistry.new @riak, @prefix
        @global_registry = GlobalRegistry.new @riak, @prefix
      end

      def clear_nodes
        @node_registry.clear
      end

      def clear_globals
        @global_registry.clear
      end

      class NodeRegistry
        def initialize(riak, prefix)
          @riak = riak
          @bucket = @riak.bucket "#{prefix}_nodes"
        end

        def get(node_id)
          @bucket.get(node_id).data
        end

        def set(node_id, addr)
          ro = @bucket.get_or_new node_id
          ro.data = addr
          ro.content_type = 'text/plain'
          ro.store          
        end

        def nodes
          @bucket.get_index '$bucket', '_'
        end

        def clear
          nodes.each {|n| @bucket.delete n }
        end
      end

      def get_node(node_id);       @node_registry.get(node_id) end
      def set_node(node_id, addr); @node_registry.set(node_id, addr) end
      def nodes;                   @node_registry.nodes end

      class GlobalRegistry
        def initialize(riak, prefix)
          @riak = riak
          @bucket = @riak.bucket "#{prefix}_globals"
        end

        def get(key)
          string = @bucket.get(key)
          Marshal.load string.data if string
        end

        def set(key, value)
          ro = @bucket.get_or_new key
          ro.data = Marshal.dump value
          ro.content_type = 'text/plain'
          ro.store
        end

        def global_keys
          @bucket.get_index '$bucket', '_'
        end

        def clear
          global_keys.each {|n| @bucket.delete n }
        end
      end

      def get_global(key);        @global_registry.get(key) end
      def set_global(key, value); @global_registry.set(key, value) end
      def global_keys;            @global_registry.global_keys end
    end
  end
end
