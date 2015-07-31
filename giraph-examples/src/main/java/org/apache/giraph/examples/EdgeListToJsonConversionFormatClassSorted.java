/**
* @job 
* The Stanford Network Analysis Platform (SNAP) contains a variety of graphs, some of them are
* the social network graphs. One of them is the Facebook graph. 
* Giraph e.g. (SingleSourceShortestPaths, PageRank) accepts input in the JsonLongDoubleFloatDoubleVertexInputFormat
* The files on SNAP are in simple Edge List Format. This program 
* converts an edge list based file format to JSON File based on the relative sorted ordering
* so that giraph jobs can be run on it.
* @input
* 	Simple Edge list format
* @output 
* 	JSON Converted Format with relative sorted ordering
*/

import java.util.Scanner;

public class EdgeListToJsonConversionFormatClassSorted{

	
	public static void convert(int[] a, int[] b, int n){
		 
	 	//int vertex_count = 0;
	 	
	 	int val = 0;
	 	int j;
	 	for(int i=0;i<n;i++){
	 	 	val = a[i];
	 	 	System.out.print("["+a[i]+",0,[["+b[i]+",1]");
	 	 	j=i+1;
	 	 	//vertex_count++;
	 	 	while(j<n && a[j] == val){
	 	 		System.out.print(",["+b[j]+",1]");
	 	 		j++;
	 	 	}
	 	 	
	 	 	System.out.println("]]");
	 	 	i = j-1;
	 	}
	 	
	 	//System.out.println("Number of vertices "+vertex_count);
	 	//System.out.println("Number of edges "+edge_count);
	 }
	
 public static void sort_relative_second(int[] a, int[] b, int n)
 {
	int i, val, j;

	i=0;

	while(i<n)
	{

	val = a[i];
	j = i+1;

	while(j<=n-1 && a[j]==val)
	{
		j++;
	}	
	for(int c1=i;c1<j-1;c1++)
	{
		int temp_key = c1;
		for(int c2=c1+1;c2<j;c2++)
		{
			if(b[c2]<b[temp_key])
				temp_key = c2;
		}
		int temp = b[temp_key];
		b[temp_key] = b[c1];
		b[c1] = temp;
	}

	i = j;

	}
	//display2(a, b, n);
	convert(a, b, n);
 }

 public static void display2(int[] a, int[] b, int n)
 {
	for(int i=0;i<n;i++)
		System.out.println(a[i]+" "+b[i]);
	System.out.println();
 }
 
 public static void display(int[] a, int n)
 {
 	for(int i=0;i<n;i++)
		System.out.print(a[i]+" ");
	System.out.println();
 }

 public static void main(String[] args){
  Scanner in = new Scanner(System.in);
  int count = 0;
  
  /**
  Calculate the count of the number of vertices pair in the graph
  */
  for(String s : args){
   count++;
  }

 /**
 Declaring the arrays to hold the input
 */
 int[] arr = new int[count];
 int[] num1 = new int[count/2];
 int[] num2 = new int[count/2];

 int parity = 0, k1=0, k2=0; 

 for(String s : args){
   arr[parity] = Integer.parseInt(s);

  if(parity%2==0)
	num1[k1++] = Integer.parseInt(s);

  else
	num2[k2++] = Integer.parseInt(s);

  parity++;
  }

 //display(arr, arr.length);
 //display(num1, num1.length);
 //display(num2, num2.length);
 //display2(num1, num2, num1.length);

 /**
 Converting the arrays into the Dual Edge Lis Based Format
 */
 int[] a = new int[count];
 int[] b = new int[count];
  
 int i;

 for(i=0;i<num1.length;i++)
 {
  	a[i] = num1[i];
	b[i] = num2[i];	
 }
 for(int k=0;k<num2.length;k++)
 {
	a[k+i] = num2[k];
	b[k+i] = num1[k];
 }

 //display2(a, b, a.length);

 /**
 Sorting the array relative to the first array
 */

 for(i=0;i<a.length-1; i++)
 {
	int key = i;
	for(int j=i+1;j<a.length;j++)
	{
		if(a[j]<a[key])
			key = j;
	}
	int temp1 = a[key];
	int temp2 = b[key];
	a[key] = a[i];
	b[key] = b[i];
	a[i] = temp1;
	b[i] = temp2;
 }

 //display2(a, b, a.length);

 /**
 Provide even more sorting to sort the indices of the second array if the index of the first array is the same
 */
  
 sort_relative_second(a, b, a.length);
 }
}