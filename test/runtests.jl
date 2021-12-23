using Test
 include("../src/WJFParallelTask.jl")

 @testset "map prefix" begin
     a = Float64[i for i = 1:10000]
     function mapFun(i1, i2)
         result = Float64[a[i] for i = i1:i2]
         for j = 2:length(result)
             result[j] += result[j - 1]
         end
         return result
     end
     function blockPrefixFun(x0::Float64, xs::Vector{Float64})
         result = Vector{Float64}(undef, length(xs))
         for i = 1:length(xs)
             result[i] = xs[i] + x0
         end
         return result
     end
     function mapFun2(i1, i2)
         return sum(a[i1:i2])
     end
     function reduceFun(xs::Vector{Float64})
         return sum(xs)
     end
     for round = 1:3000
         b = WJFParallelTask.mapPrefix(
             1,length(a),10, mapFun, blockPrefixFun, Float64[10])
         c = WJFParallelTask.mapReduce(
             1,length(a),10, mapFun2, reduceFun, 10.0
         )
         aResult = Vector{Float64}(undef, length(a) + 1)
         aResult[1] = 10
         function loopBody(i1, i2)
             for i = i1:i2
                 aResult[i + 1] = aResult[1] + sum(1:i)
             end
         end
         WJFParallelTask.mapOnly(
             1,length(a),10,loopBody
         )
         err = sum(abs.(aResult .- b))
         if round % 100 == 0
             println("round = $round, err = $err, c = $c")
         end
         @test err ≈ 0
         @test c ≈ aResult[end]
     end

 end
