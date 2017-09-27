/*
A recursive class that prints string format for a given number.
*/

class ChangeIntegersToStrings {
var a = scala.io.StdIn.readLine("Enter any number in range 1 -> 9999 to print out the string format" + "\n")
var Inputvar:Int = a.toInt
val unit:List[String] = List(" ","one","two","three","four","five","six","seven","eight","nine","ten","eleven","twelve","thirteen","fourteen","fifteen","sixteen","seventeen","eighteen","nineteen")
val TwentyToNinty:List[String] = List(" "," ","twenty","thirty","forty","fifty","sixty","seventy","eighty","ninty")

// var Check:Boolean = true

/*
Define a recursive procedure calculating the length of integers entered by the User.
*/

def GiveLength(a:Int, i:Int = 1):Int = {
	if (a  < 10 ) i 
	else GiveLength(a / 10, i + 1) 
	}
// println(GiveLength(Inputvar))


/*
Convert is a recursive function that uses user (i) input integer of range 1 -> 9999, and uses Givelength method to retrieve the (i) integer size.
*/

/*
	Convert function expects return type String.
	Note: However, the if /else if statements don't seem to return String, rather Unit().
	Temporary fix: add else-statement to return string 
*/

// Notes: In scala declaration and assignments are of type Unit. So a 'void' function is of type Unit.


def Convert(temp_int: Int):String = {
 	Inputvar = temp_int

 	if (GiveLength(temp_int) == 1 || GiveLength(temp_int) == 2){
 		if(temp_int < 20 && temp_int >= 0){
 			this.unit(temp_int)
 		}
 		else if(temp_int >= 20 && temp_int < 100){
 			this.TwentyToNinty(temp_int / 10 ) + " "+ this.unit(temp_int % 10) 
 		}

 		else {""}
 	} 
 	
 	else if (GiveLength(temp_int) == 3){
 		if(temp_int >= 100 && temp_int < 1000){
 			this.unit(temp_int / 100 ) + " hundred "+ Convert(temp_int % 100 )
 		} else {""}
 	}
 	
 	else if  (GiveLength(temp_int) == 4){
 		if(temp_int >= 1000 && temp_int < 10000){
 			this.unit(temp_int / 1000 ) + " thousand "+ Convert(temp_int % 1000 )
 		} else {""}
 	} else {""}
}
println(Convert(this.Inputvar))
}


var T = new ChangeIntegersToStrings();



