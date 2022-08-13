import java.io.*;
import java.util.*;

class FileHandling
{
    public static void generateInitialData () throws IOException
    {
        File original = new File("original.txt");
        if (original.createNewFile())
        {
            System.out.println("new file create");
        }
        else
        {
            System.out.println("file already exists");
        }
    }
}

public class ExternalSort
{
    public static void main (String[] args) throws IOException
    {
        FileHandling.generateInitialData();
    }
}