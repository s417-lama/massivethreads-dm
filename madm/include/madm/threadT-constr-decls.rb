
open("threadT-constr-decls.h", "w") do |f|
  8.times do |i|
    f.puts <<EOS
        template <class F#{ (0...i).map{|x| ", class T#{x}"}.join }>
        explicit thread(F f#{ (0...i).map{|x| ", T#{x}& arg#{x}"}.join });
EOS
  end
end
