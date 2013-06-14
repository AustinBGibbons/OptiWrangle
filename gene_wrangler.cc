#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <bitset>
#include "omp.h"
#include <time.h>
#include <math.h>

using namespace std;

#define FILE_WIDTH 4

int THREADS = 1;
int gene_processing(string *** table, int length, int width) ;
map<string, vector<string *> *> *gene_partitioning(string **table, int length, int width);

void goodbye(string msg) {
  cout << msg << endl;
  exit(1);
}

// this version tries to more closely mirror what
// wrangler does

int main(int argc, char *argv[]) {
  printf("Hello World\n");
  if (argc < 4) goodbye("pass in the fastq files and number of THREADS, yo");
  THREADS = atoi(argv[3]);
  omp_set_num_threads(THREADS);
  ifstream fastq ; //= argv[0];  
  fastq.open (argv[1], ios::in);
  if (! fastq.is_open()) goodbye("This file is not open, yo.");
  string line;
  int file_length = -1; // -1 for EOF
  while( fastq.good() ) {
    getline(fastq, line);
    // do something
    file_length++;
  }
  fastq.close();
  printf("lines in file: %d\n", file_length);

  //string table[file_length][FILE_WIDTH];
  string **table = new string*[file_length];
  for(int i = 0; i < file_length; i++) table[i] = new string[FILE_WIDTH];
  fastq.open (argv[1], ios::in);
  if (! fastq.is_open()) goodbye("This file is not open, yo.");
  for(int i = 0; i < file_length; i++) {
    getline(fastq, line);
    stringstream ss(line);
    int line_width = 0;
    string token;
    vector<string> v;
    while(getline(ss, token, '\t')) {
      line_width++;
      v.push_back(token);
 //     cout << "elem: " << table[i][line_width] << endl;
    }
    if(line_width != 4) goodbye("Bad line");
    copy(v.begin(), v.end(), table[i]);
  }
  fastq.close();

  // wow that was painful

  // clock
    clock_t t;
    t = clock();
/*
    map<string, vector<string *> *> *partitions = gene_partitioning(table, file_length, FILE_WIDTH);
    // I probably could have just done pragma omp parallel for here,
    // #fml
    for( map<string, vector<string *> *>::iterator it = partitions->begin(); 
    it != partitions->end(); ++it) {
      //vector<string *> *curr = it->second;
      string **curr = (string **) &(*(it->second))[0];
      //cout << "Hello World" << endl;
      //cout << "Key: " << it->first << endl;
      int new_length = gene_processing( &curr, it->second->size(), FILE_WIDTH);
      //cout << "Goodybe, cruel World" << endl;
      // just checking ...
      if (new_length > 3) cout << table[3][1] << endl;
      cout << new_length << endl;
    }
*/
    cout << "gene_processing" << endl; 
    int new_length = gene_processing(&table, file_length, FILE_WIDTH);
    cout << new_length << endl;

  // clock
  t = clock() - t;
  printf ("Time : %f seconds.\n",((float)t)/CLOCKS_PER_SEC);

  // write to file
  ofstream out;
  out.open (argv[2], ios::out);
  for (int i = 0; i < new_length; i++) { 
    for (int j = 0; j < FILE_WIDTH-1; j++) {
      out << table[i][j] << "\t"; 
    } 
    out << table[i][FILE_WIDTH-1] << endl;
  }
  return 0;
}

//string g = "AGATCGGAAGAGCGGTTCAGCAGGAATGCCGAGACCGATCTCGTATGCCGTCTTCTGCTTG";

bool clip(string seq) {
  string g = "AGAT";//CGGAAGAGCGGTTCAGCAGGAATGCCGAGACCGATCTCGTATGCCGTCTTCTGCTTG";
  int GENE_SIZE = g.size();
  if(seq.size() < GENE_SIZE) return false;
  bool match = true;
  for(int i = 0; i < g.size(); i++) {
    if (seq[i] != 'N' && seq[i] != g[i]) {
      match = false;
      break ;
    }
  }
  return match; 
}

// maybe its worth it do a scan first?
/*
map<string, vector<string *> *> *gene_partitioning(string **table, int length, int width) {
  map<string, vector<string *> *> *partitions = new map<string, vector<string *> *>();
  #pragma omp parallel for shared(partitions)
  for (int t = 0; t < THREADS; t++) {
    map<string, vector<string *> *> local_partitions;
    int lower = (t*length) / THREADS;
    int upper = length;
    if(t != THREADS - 1) upper = ((t+1) / length) / THREADS;
    for (int i = lower; i < upper; i ++) {
      if (table[i][1].size() < 8) continue ;
      string key = table[i][1].substr(4,4);
      vector<string *> *curr;
      if (local_partitions.find(key) == local_partitions.end()) curr = new vector<string *>();
      else curr = local_partitions.find(key)->second;
      curr->push_back(table[i]);
      local_partitions[key] = curr; // is this necessary? TODO
    }
    //#pragma omp single
    for( map<string, vector<string *> *>::iterator it = local_partitions.begin(); 
    it != local_partitions.end(); ++it) {
      vector<string *> *merged_partitions = new vector<string *>();
      vector<string *> *curr;
      if ( partitions->find(it->first) == partitions->end()) curr = new vector<string *>();
      else curr = partitions->find(it->first)->second;
      merged_partitions->reserve(it->second->size() + curr->size());
      merged_partitions->insert(merged_partitions->end(), curr->begin(), curr->end());
      merged_partitions->insert(merged_partitions->end(), it->second->begin(), it->second->end());
      (*partitions)[it->first] = merged_partitions;
    }
  }
  
  return partitions;
}
*/
int deleteClipper(string *** table, int _length, int _width);
void cutBefore(int index, string *** table, int _length, int _width);
int gene_processing(string *** table, int _length, int _width) { 
  cout << "start" << endl;
  int new_length = deleteClipper(table, _length, _width);
  cout << "delete" << endl;
  cutBefore(13, table, new_length, _width);
  cout << "cutBefore" << endl;
  return new_length;
}

int deleteClipper(string *** table, int _length, int _width) {  
  vector<string*> *new_table = new vector<string *>;
  cout << "length: " << _length << endl;
  cout << "THREADS: " << THREADS << endl;
  #pragma omp parallel for //shared(new_table)
  for(int t = 0; t < THREADS; t++) {
    //vector<string *> local_table;// = new vector<string *>();
    int lower = (t*_length) / THREADS;
    int upper = _length;
    if(t != THREADS - 1) upper = ((t+1) * _length) / THREADS;
    for (int i = lower; i < upper; i ++) {
      //if(clip((*table)[i][1])) local_table.push_back((*table)[i]);
      if(!clip((*table)[i][1])) new_table->push_back((*table)[i]);
    }
    
 //   new_table->insert(new_table->end(), local_table.begin(), local_table.end());
  }
  
  (*table) = &(*new_table)[0];
  return new_table->size();
}

void cutBefore(int index, string *** table, int _length, int _width) {
  //string **newtable = new string*[new_length];
  // such little data here, maybe un parallelize
  //#pragma omp parallel for
  for (int i = 0; i < THREADS; i++) {
    int lower = (i*_length) / THREADS;
    int upper_bound = _length;
    if (i != THREADS - 1) upper_bound = ((i+1)*_length)/THREADS; // offsets[i+1];
    for(int j = lower; j < upper_bound; j++) {
      if ((*table)[j][1].size() >= 13)  {
        (*table)[j][1] = (*table)[j][1].substr(13);
      }
    }
  }
  //(*table) = newtable;
}

/*
  int length = _length;
  int width = _width;


  // conditionally delete on table[*][1]
  // maybe its worth it to scan once and allocate...
  // vector<string*> *new_table = new vector<string *>;
  //bitset <length> filter;
  size_t ulong = 64; //sizeof(unsigned long);
  size_t filter_size = (length + ulong -1 ) / ulong;
  //cout << "into: " << length << endl; 
  unsigned long *filter = new unsigned long[filter_size];
  int new_length = 0;
  #pragma omp parallel for shared(new_length, filter)
  for(int t = 0; t < THREADS; t++) {
    int local_counter = 0;
    int lower = (t*filter_size) / THREADS;
    int upper = filter_size;
    if(t != THREADS - 1) upper = ((t+1) / filter_size) / THREADS;
    for (int i = lower; i < upper; i += 64) {
      // check this before you run it on your computer
      int end = ulong;
      if (i+end > length) end = length;
      bitset<64> bit_filter;
      for(int j = 0 ; j < end; j++) {
        bit_filter[j] = clip((*table)[i+j][1]);
      }
      filter[i / ulong] = bit_filter.to_ulong();
      local_counter += bit_filter.count();
    }
    new_length += local_counter;
  }

  if (new_length == 0) return 0; //goodbye("Ain't nothin' to see here");
  string **newtable = new string*[new_length];
  // such little data here, maybe un parallelize
  #pragma omp parallel for
  for (int i = 0; i < THREADS; i++) {
    int lower = (i*length) / THREADS;
    int upper_bound = length;
    if (i != THREADS - 1) upper_bound = (((i+1)*length)/THREADS); // offsets[i+1];
    int local_counter = 0;
    for(int j = lower; j < upper_bound; j++) {
      bitset<64> bit_filter(filter[j / ulong]);
      bool isSet = bit_filter[j % ulong];
      if(isSet) {
        newtable[local_counter] = (*table)[j];
        if (newtable[local_counter][1].size() >= 13)  {
          newtable[local_counter][1] = newtable[local_counter][1].substr(13);
        }
        local_counter++;
      }
    }
  }
  // free memory as desired...
  // delete table etc.
  (*table) = newtable;
  return new_length;
}
*/
