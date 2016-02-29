import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by alanbuzdar on 2/28/16.
 */
public class Client {
    public static void main(String[] args){

        try {
            Socket sOne = new Socket("localhost", 8001);
            Socket sTwo = new Socket("localhost", 8002);
            Socket sThree = new Socket("localhost", 8003);

            PrintWriter one = new PrintWriter(sOne.getOutputStream());
            PrintWriter two = new PrintWriter(sTwo.getOutputStream());
            PrintWriter three = new PrintWriter(sThree.getOutputStream());

            Scanner reader1 = new Scanner(sOne.getInputStream());
            Scanner reader2 = new Scanner(sTwo.getInputStream());
            Scanner reader3 = new Scanner(sThree.getInputStream());

            one.println("key4 value4");
            one.flush();
            two.println("key4 value4");
            two.flush();
            three.println("key4 value4");
            three.flush();

            System.out.println("get here");
            String s1 = reader1.nextLine();
            String s2 = reader2.nextLine();
            String s3 = reader3.nextLine();
            System.out.println("we get here");
            System.out.println(s1);
            if (s1.contains("Accept") && s2.contains("Accept") && s3.contains("Accept")) {
                one.println("Commit");
                two.println("Commit");
                three.println("Commit");
                one.flush();
                two.flush();
                three.flush();
                System.out.println("and here");
            }

            s1 = reader1.nextLine();
            s2 = reader2.nextLine();
            s3 = reader3.nextLine();

            System.out.println("but here?");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
