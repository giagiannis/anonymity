#!/usr/bin/ruby


#	alter the following array, to change the ranges of the attributes

my_ranges=	[	[0,1],
				[1,100],
				[10000,99999],
				[10, 100],
				[1, 5000],
				[1, 10],
				[1, 4],
				[1,1000]
			]

#	default number of lines. normally, lines are given as an argument to the script
#	if ARGV[0] is nil, then the default number is used
number_of_lines=1000



class MyDataGen
	def initialize(rows, ranges)
		@rows=rows.to_i
		@range = ranges
	end

	def print_att(x, endofline)
		print(x[0]+rand(x[1]-x[0]+1))
		print endofline
	end

	def print_line 
		@range.each{ |x|
			if(x==@range.last)
				print_att(x,"\n")
			else
				print_att(x,", ")
			end
		}
	end

	def get_output
		@rows.times{	|x|
			print_line
		}
	end
end

if(ARGV[0]!=nil)
	number_of_lines=ARGV[0].to_i
end

a = MyDataGen.new(number_of_lines,my_ranges)
a.get_output
