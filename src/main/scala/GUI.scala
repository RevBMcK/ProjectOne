import java.awt._
import javax.swing._

object GUI extends App {

  val f = new JFrame //creating instance of JFrame
  val b = new JButton("click") //creating instance of JButton
  b.setBounds(130, 100, 100, 40) //x axis, y axis, width, height

  f.add(b) //adding button in JFrame

  f.setSize(400, 500) //400 width and 500 height

  f.setLayout(null) //using no layout managers

  f.setVisible(true) //making the frame visible

  /*val textArea = new JTextArea("Hello, Swing world")
  val scrollPane = new JScrollPane(textArea)

  val frame = new JFrame("Hello, Swing")
  frame.getContentPane.add(scrollPane, BorderLayout.CENTER)
  frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
  frame.setSize(new Dimension(600, 400))
  frame.setLocationRelativeTo(null)
  frame.setVisible(true)*/

}
