/* This is a comment.
First working class in Scala.
*/

class FIRST {
var A:Int=0
var B:Int=0
var Name:String = "You"
def Welcome() = {
println("Welcome!" + Name + "\n");
}

}

var XYZ = new FIRST();
XYZ.A = 10;
XYZ.B = 15;

// Print sum of two instance variables.
println(XYZ.A + XYZ.B + "\n"
);

// Make a reference variable 'name' to FIRST Object.
var name = new FIRST();
// Show Welcome message in addition to instance string as assigned in class scope.

name.Welcome();
// Over-write class assigned string with any input string.
name.Name = "Input-String";
name.Welcome();


class Math{
def Subtract(A:Int, B:Int):Int={
A-B;
}
def Multiply(C:Int, D:Int):Int = {
D * C;}
/* Note: A float value must have 'f' as a postfix*/
def FloatDivide(E:Float,F:Float):Float= {
E / F; 
}
}

var Result = new Math();
println(Result.Subtract(10,5));
println(Result.Multiply(2,5));
println(Result.FloatDivide(1.2f,5.15f));



/* In scala, constructor isn't explicitly typed, but is implicitly made within ( )s.
new method makes an object.
*/


class PrintAdd(A:Int, B:Int) {


def Add():String = {
"The result is: " + (A+B) + "\n";
}
def Subtract(): String = {
"The result is: " + (A-B) + "\n";}

println("Some String");
// Above print statement has it's own constructor within ( )s. 
}

var RTE = new PrintAdd(5,10);
println(RTE.Add());
println(RTE.Subtract());

// var has get/set method.
// val just set it.

         

// Demonstration on the usage of contstuctors including constructor overloading.
class Constructor_class(A:Int) {
		println("Primary consutructor executed !");
		var C = true
		// Add a for loop for int value
		// Note the FOR syntax!
		for( B <- A to 10){
			println("The current number is: " +B);

			// 	Add while loop within the for loop
			while( C == true){
				if (B==6){
					println("Line executed until" +B);
					}
				else {
					C = false;
					println("While stopped at:" + B)
					};
				
				
			}

		}

	

		def this(B:Int,X:Int) = {
			this(5);
			println("First auxillary constructor executed !")
			println(B+X + "\n");

		}
		def this(D:Int, Y:String) = {
			this(4,5);
			println("Secondary auxillary constructor executed ! ..")
		}

}
//var TRE= new Constructor_class(5);
var TRET = new Constructor_class(2,"random"); // Note: Supplying last constructor parameters leads to execution of all.

