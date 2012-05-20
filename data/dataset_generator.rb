#!/usr/bin/ruby

class DataSetGenerator
	def initialize rows
		@rows=rows
		@sex=["Male","Female"]
		@disease=	[	"Flu",
						"Hepatitis",
						"Brochitis",
						"Broken Arm",
						"AIDS",
						"Hang Nail",
						"Asthma",
						"Appendicitis",
						"Chickenpox",
						"Colitis",
						"Parkinson",
						"Typhus",
						"Malaria"
					]
	end
	def create_line
		print 20+rand(50)
		print ','
		print @sex[rand(2)]
		print ','
		print 53000+rand(@rows/2)
		print ','
		print @disease[rand(@disease.length)]
		puts
	end
	def output
		(1..@rows).each {|x|
			create_line
		}
	end
end

return 0 if ARGV.length<1
foo=DataSetGenerator.new(ARGV[0].to_i)
foo.output
