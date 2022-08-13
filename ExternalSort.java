import java.io.*;
import java.util.*;

class UtilityFunctionClass
{
    static String createDiskBlocksWithData (List<String> data, int blockSize, int pass) throws IOException
    {
        int curRun = 0;
        int totalRuns = (int) Math.ceil((double) data.size() / (double) blockSize);

        while (curRun < totalRuns)
        {
            File temp = new File ("secondaryMemory/pass_" + pass + "_run_" + curRun + "_data.txt");
            temp.createNewFile();
            FileWriter fwrite = new FileWriter(temp);

            if ((curRun+1)*blockSize < data.size())
            {
                for (int i = 0; i < blockSize; i++)
                {
                    fwrite.write(data.get(curRun*blockSize + i) + "\n");
                }
                fwrite.write("secondaryMemory/pass_" + pass + "_run_" + (curRun+1) + "_data.txt");
            }
            else
            {
                for (int i = curRun*blockSize; i < data.size(); i++)
                {
                    fwrite.write(data.get(i) + "\n");
                }
                fwrite.write("END_OF_RUN");
            }
            curRun++;
            fwrite.close();
        }

        return ("pass_" + pass + "_run_0_data.txt");
    }

    static void cleanDirectory (File dir)
    {
        for (File file : dir.listFiles())
        {
            if (!file.isDirectory())
            {
                file.delete();
            }
        }
    }
}

// used only once to create a single txt file containing 50000 random records.
class InitialFileCreation
{
    public static void generateInitialData () throws IOException
    {
        UtilityFunctionClass.cleanDirectory(new File ("mainMemory"));
        UtilityFunctionClass.cleanDirectory(new File ("secondaryMemory"));

        File original = new File("original.txt");
        original.createNewFile();
        if (original.exists())
        {
            Random random = new Random();
            FileWriter fwrite = new FileWriter(original);
            for (int index = 1; index <= 50000; index++)
            {
                int saleAmount = random.nextInt(60001);
                String customerName = generateRandomString();
                int category = random.nextInt(1501);

                fwrite.write(index + " " + saleAmount + " " + customerName + " " + category + "\n");
            }
            fwrite.close();
        }
    }

    private static String generateRandomString ()
    {
        StringBuilder buffer = new StringBuilder(3);
        int alphabetStart = 97;
        int alphabetEnd = 122;
        for (int i = 0; i < 3; i++)
        {
            int randomInt = (int) (Math.random() * (alphabetEnd - alphabetStart + 1)) + alphabetStart;
            buffer.append((char) randomInt);
        }

        return (buffer.toString());
    }
}

public class ExternalSort
{
    public static void main (String[] args) throws IOException
    {
        InitialFileCreation.generateInitialData();

        Scanner userInput = new Scanner(System.in);
        File file = new File ("original.txt");
        Scanner fRead = new Scanner (file);
        List<String> fReadData = new ArrayList<>();

        while (fRead.hasNextLine())
        {
            fReadData.add(fRead.nextLine());
        }

        System.out.println("Enter the number of records in a disk block (B)");
        int blockSize = userInput.nextInt();
        System.out.println("Enter the number of disk blocks in main memory (M)");
        int mainMemSize = userInput.nextInt();

        String firstFileName = UtilityFunctionClass.createDiskBlocksWithData(fReadData, blockSize, 0);

        userInput.close();
        fRead.close();

        externalSort(firstFileName, blockSize, mainMemSize);
    }

    static void externalSort (String firstFileName, int blockSize, int mainMemSize)
    {

    }

    // private static void debugger ()
    // {
        
    // }
}