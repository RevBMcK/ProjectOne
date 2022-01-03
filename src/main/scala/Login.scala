
object Login extends App {

  print("Input username: ")
  var userName = scala.io.StdIn.readLine()

  print("Input password: ")
  var password = scala.io.StdIn.readLine()

  def start(a: String, b: String) = {
    println("Hello " + a + ". Your password is " + b)
  }

  if (userName == "Bob" && password == "123") {
    println("Welcome")
    start(userName, password)
  }
  else {
    println("Invalid user")
  }
}
