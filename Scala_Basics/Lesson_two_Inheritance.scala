// Implementation of Inheritance

class One {
	// During inheritance this will be executed first
	// {
	// 	println("Hello ! From class One")
	// }
	def Message1()={
		println("Message 1 printed ")
	}

}

class Two extends One {
	// During inheritance this will be executed second
	// {
	// 	println("Hello ! From class Two")
	// }
	def Message2()={
		println(" Message 2 printed ")

	}

}

var ref_to_one:One = new Two();
ref_to_one.Message1 // This line will print parent class method.
//ref_to_one.Message2 // This line will not print, due to variable referring to data type One !
var ref_to_two:Two = new Two();
ref_to_two.Message2 