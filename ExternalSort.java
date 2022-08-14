import java.io.*;
import java.util.*;

class UtilityFunctionClass
{
    static String createDiskBlocksWithData (List<String> data, int blockSize, int pass, boolean isUnsorted) throws IOException
    {
        int curRun = 0;
        int totalRuns = (int) Math.ceil((double) data.size() / (double) blockSize);
        String str = (isUnsorted) ? "_unsortedData.txt" : "_data.txt" ;
        while (curRun < totalRuns)
        {
            File temp = new File ("secondaryMemory/pass_" + pass + "_run_" + curRun + str);
            temp.createNewFile();
            FileWriter fwrite = new FileWriter(temp);

            if ((curRun+1)*blockSize < data.size())
            {
                for (int i = 0; i < blockSize; i++)
                {
                    fwrite.write(data.get(curRun*blockSize + i) + "\n");
                }
                fwrite.write("END_OF_RUN\n");
                fwrite.write("secondaryMemory/pass_" + pass + "_run_" + (curRun+1) + str);
            }
            else
            {
                for (int i = curRun*blockSize; i < data.size(); i++)
                {
                    fwrite.write(data.get(i) + "\n");
                }
                fwrite.write("END_OF_BLOCKS");
            }
            curRun++;
            fwrite.close();
        }

        return ("secondaryMemory/pass_" + pass + "_run_" + 0 + str);
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

    static List<String> sortDiskBlock (List<String> data)
    {
        List<String> sortedData = new ArrayList<>();
        Map<Integer, String> mappedData = new HashMap<>();
        List<Integer> transactionData = new ArrayList<>();

        for (String str : data)
        {
            String[] temp = str.split(" ");
            transactionData.add(Integer.parseInt(temp[1]));
            mappedData.put(Integer.parseInt(temp[1]), str);
        }

        Collections.sort(transactionData);

        for (int x : transactionData)
        {
            sortedData.add(mappedData.get(x));
        }

        return sortedData;
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

        String firstFileName = UtilityFunctionClass.createDiskBlocksWithData(fReadData, blockSize, 0, true);

        userInput.close();
        fRead.close();

        externalSort(firstFileName, blockSize, mainMemSize);
    }

    static void externalSort (String firstFileName, int blockSize, int mainMemSize) throws FileNotFoundException
    {
        applySortingToInitialRuns (firstFileName, blockSize, mainMemSize);
    }

    static void applySortingToInitialRuns (String firstFileName, int blockSize, int mainMemSize) throws FileNotFoundException
    {
        String nextBlockPtr = firstFileName;

        // outer list contains 'mainMemSize' inner lists - representing disk blocks
        // each inner list contains 'blockSize' records - representing records in a disk block
        List<List<String>> simulatedMainMemory = new ArrayList<List<String>>();
        
        while (!nextBlockPtr.equals("END_OF_BLOCKS"))
        {
            int tempCounter = mainMemSize;
            while (tempCounter > 0 && !nextBlockPtr.equals("END_OF_BLOCKS"))
            {
                File file = new File (nextBlockPtr);
                Scanner fRead = new Scanner (file);
                List<String> fReadData = new ArrayList<>();
                
                while (fRead.hasNextLine())
                {
                    fReadData.add(fRead.nextLine());
                }

                nextBlockPtr = fReadData.get(fReadData.size()-1);
                fReadData.remove(fReadData.size()-1);
                fReadData.remove(fReadData.size()-1);

                simulatedMainMemory.add(fReadData);

                tempCounter--;

                fRead.close();
            }

            for (int i = 0; i < simulatedMainMemory.size(); i++)
            {
                simulatedMainMemory.set(i, UtilityFunctionClass.sortDiskBlock(simulatedMainMemory.get(i)));
            }
        }

        // write the sorted records back to the file and move to the next file
    }

    // private static void debugger ()
    // {
        
    // }
}