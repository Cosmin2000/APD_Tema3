#include<mpi.h>
#include<stdio.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>

#define min(a,b) (((a)<(b))?(a):(b))

void read_workers(int rank, int *my_nr_workers, int **workers) {
    FILE *fp;
    char file_name[15];
    sprintf(file_name, "./cluster%d.txt", rank);

    fp = fopen(file_name, "r");
	fscanf(fp, "%d", my_nr_workers);

	*workers = malloc(sizeof(int) * (*my_nr_workers));

	for (size_t i = 0; i < (*my_nr_workers); i++)
		fscanf(fp, "%d", &(*workers)[i]);

}

int isCoordonator(int rank) {
	int a = (rank == 0 || rank == 1 || rank == 2) ? 1 : 0;
	return a;
}

char printTopology(int rank, int **topology, int nProcesses) {
	char result[200];
		sprintf(result,"%d ->", rank);

		for (int i = 0; i < 3; i++) {
			char tmp[100], tmp1[100];
			if (topology[i][0] != 0) {
				sprintf(tmp," %d:%d", i, topology[i][0]);
			}
			for (int j = 1 ; j < (nProcesses - 3) && topology[i][j] != 0; j++) {
				sprintf(tmp1,",%d", topology[i][j]);
				strcat(tmp,tmp1);
			}
			strcat(result, tmp);
		}
		printf("%s\n", result);
}

int main(int argc, char * argv[]) {
	int rank, nProcesses, num_procs, coordonator;
	int *parents, **topology;
	int N, eroare_comunicatie, dim;
	int my_nr_workers;
	int *my_workers;
	int nr_workers_coord[2];
	int *workers_coord[2];
	int *V;
	int myRank;
	int vec_id;
	int nr_total_workers;
	int start, end;

	MPI_Init(&argc, &argv);
	MPI_Status status;
	MPI_Request request;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);
	nr_total_workers = nProcesses - 3;


	if (argc < 2) {
		printf("./tema3 <dimensiune_vector> <eroare_comunicatie>\n");
	}

	if (rank == 0) {
		N = atoi(argv[1]);
	}
	eroare_comunicatie = atoi(argv[2]);

	topology = (int**)malloc(sizeof(int*) * 3);
	for (int i = 0; i < 3; i++) {
		topology[i] = (int*)calloc(sizeof(int), (nProcesses - 3));
	}

	//=========================== STABILIREA TOPOLOGIEI ====================
	if (isCoordonator(rank)) {
		read_workers(rank, &my_nr_workers, &my_workers);
	}

	MPI_Barrier(MPI_COMM_WORLD);

	if (isCoordonator(rank) && rank != 0) {
		//Trimit la coordonatorul 0
		MPI_Send(&my_nr_workers,1, MPI_INT,0, rand() % 10,MPI_COMM_WORLD);
	 	MPI_Send(my_workers,my_nr_workers, MPI_INT,0,rand() % 10,MPI_COMM_WORLD);
		for (int i = 0; i < 3; i++) {
			MPI_Recv(topology[i], nProcesses - 3,MPI_INT, 0, MPI_ANY_TAG ,MPI_COMM_WORLD, &status);
			printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		}
		printTopology(rank,topology, nProcesses);

		//Trimitem la workeri acestui coordonator
		for (int i = 0; i < my_nr_workers; i++) {
			for (int j = 0 ; j < 3; j++) {
				MPI_Send(topology[j],  nProcesses - 3, MPI_INT, my_workers[i], rand() % 10, MPI_COMM_WORLD);
			}
		}
	}

	if (rank == 0) {
		// Pimesc de la ceilalti coordonatori vecinii lor
		MPI_Status status;
		MPI_Recv(&nr_workers_coord[0],1,MPI_INT, (rank + 1)%3, MPI_ANY_TAG ,MPI_COMM_WORLD, &status);
		workers_coord[0] = (int*)malloc(sizeof(int)*nr_workers_coord[0]);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		MPI_Recv(workers_coord[0],nr_workers_coord[0],MPI_INT, (rank + 1)%3, MPI_ANY_TAG ,MPI_COMM_WORLD, &status);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		MPI_Recv(&nr_workers_coord[1],1,MPI_INT, (rank + 2)%3, MPI_ANY_TAG ,MPI_COMM_WORLD, &status);
		workers_coord[1] = (int*)malloc(sizeof(int)*nr_workers_coord[1]);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		MPI_Recv(workers_coord[1],nr_workers_coord[1],MPI_INT, (rank + 2)%3, MPI_ANY_TAG ,MPI_COMM_WORLD, &status);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);

		//Creez matricea de topologie
		for (int i = 0; i < nr_workers_coord[0]; i++) {
			topology[(rank + 1) % 3][i] = workers_coord[0][i];
		}
		for (int i = 0; i < nr_workers_coord[1]; i++) {
			topology[(rank + 2) % 3][i] = workers_coord[1][i];
		}
		for (int i = 0; i < my_nr_workers; i++) {
			topology[rank][i] = my_workers[i];
		}

		printTopology(rank,topology, nProcesses);

		//Trimitem  la ceilalti coordonatori	
		for (int j = 0 ; j < 3; j++) {
			MPI_Send(topology[j],  nProcesses - 3, MPI_INT, (rank + 1)%3, rand() % 10, MPI_COMM_WORLD);
			MPI_Send(topology[j],  nProcesses - 3, MPI_INT, (rank + 2)%3, rand() % 10, MPI_COMM_WORLD);
		}
		

		//Trimitem la workeri acestui coordonator
		for (int i = 0; i < my_nr_workers; i++) {
			for (int j = 0 ; j < 3; j++) {
				MPI_Send(topology[j],  nProcesses - 3, MPI_INT, my_workers[i], rand() % 10, MPI_COMM_WORLD);
			}
		}
	}
	
	if (!isCoordonator(rank)) {
		//fiecare worker primeste matricea de topologie
		for (int i = 0; i < 3; i++) {
			MPI_Recv(topology[i], nProcesses - 3,MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG ,MPI_COMM_WORLD, &status);
			printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		}
		coordonator =  status.MPI_SOURCE;
		printTopology(rank,topology, nProcesses);
	}
	//================================== REALIZAREA CALCULELOR ==============================

	if (rank == 0) {
		V = (int*)malloc(sizeof(int)*N);
		for (int i = 0; i < N; i++) {
			V[i] = i;
		}
	
		// Trimit workerilor Coordonatorului 0
		for (int i = 0; i < my_nr_workers; i++) {
			start = i * (double)N / nr_total_workers;
			end = min((i + 1) * (double)N / nr_total_workers, N);
			dim = end - start;
			// trimit id-ul partii din array
			MPI_Send(&i,1, MPI_INT, my_workers[i], rand() % 10, MPI_COMM_WORLD);
			// Trimit dimensiunea si apoi datele
			MPI_Send(&dim,1, MPI_INT, my_workers[i], rand() % 10, MPI_COMM_WORLD);
			MPI_Send(V + start, dim , MPI_INT, my_workers[i], rand() % 10, MPI_COMM_WORLD);
			
		}
		vec_id = my_nr_workers;

		//Trimit celorlalti coordonatori
		
		int start1,end1, start2,end2;
		//Primul coordonator
		start1 = vec_id * (double)N / nr_total_workers;
		end1 = min((vec_id + nr_workers_coord[0]) * (double)N / nr_total_workers, N);
		dim = end1 - start1;
		// ID-ul corespunzator partii din vector
		MPI_Send(&vec_id,1, MPI_INT,(rank + 1) % 3, rand() % 10,MPI_COMM_WORLD);
		// trimit vectorul(prima data dimensiunea apoi datele)
		MPI_Send(&dim,1, MPI_INT,(rank + 1) % 3, rand() % 10,MPI_COMM_WORLD);
		MPI_Send(V + start1,dim, MPI_INT,(rank + 1) % 3, rand() % 10,MPI_COMM_WORLD);
		
		//Al doilea coordonator
		vec_id += nr_workers_coord[0];

		start2 = vec_id * (double)N / nr_total_workers;
		end2 = min((vec_id + nr_workers_coord[1] + 1) * (double)N / nr_total_workers, N);
		dim = end2 - start2;

		// ID-ul partii corespunzatoare din vector
		MPI_Send(&vec_id,1, MPI_INT,(rank + 2) % 3, rand() % 10,MPI_COMM_WORLD);
		// Trimit vectorul(prima data dimensiunea apoi datele)
		MPI_Send(&dim,1, MPI_INT,(rank + 2) % 3, rand() % 10,MPI_COMM_WORLD);
		MPI_Send(V + start2,dim, MPI_INT,(rank + 2) % 3, rand() % 10,MPI_COMM_WORLD);

		int recv_mess = 0, dim_recv = 0, id_recv = 0; // Numarul de mesaje primite, dimensiunea primita, id-ul din vector

		while (recv_mess < my_nr_workers + 2) {
			// Primesc ID-ul bucatii din vector
			MPI_Recv(&id_recv, 1,MPI_INT, MPI_ANY_SOURCE,  MPI_ANY_TAG,MPI_COMM_WORLD, &status);
			printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
			// Apoi dimensiunea
			MPI_Recv(&dim_recv, 1,MPI_INT, status.MPI_SOURCE,  MPI_ANY_TAG,MPI_COMM_WORLD, NULL);
			printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
			start = id_recv * (double)N / nr_total_workers;
			// Apoi datele
			MPI_Recv(V + start, dim_recv,MPI_INT, status.MPI_SOURCE,  MPI_ANY_TAG,MPI_COMM_WORLD, NULL);
			printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
			recv_mess++;
		}
		MPI_Barrier(MPI_COMM_WORLD); // pentru sincronizare pe stdout
		printf("Rezultat:");
		for (int i = 0; i < N; i++) {
			printf(" %d", V[i]);
		}
		printf("\n");

		free(my_workers);
		free(workers_coord[0]);
		free(workers_coord[1]);
		
	}

	// Ceilati coordonatori (1 si 2)
	if (isCoordonator(rank) && rank != 0) {
		// Primesc ID-ul bucatii din vector si apoi dimensiunea si datele efective
		MPI_Recv(&vec_id, 1,MPI_INT, 0,  MPI_ANY_TAG,MPI_COMM_WORLD, &status);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		MPI_Recv(&N, 1,MPI_INT, 0,  MPI_ANY_TAG,MPI_COMM_WORLD, &status);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		V = malloc(sizeof(int) * N);
		MPI_Recv(V, N,MPI_INT, 0,  MPI_ANY_TAG,MPI_COMM_WORLD, &status);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);

		// Trimit workerilor coordonatorului curent
		for (int i = 0; i < my_nr_workers; i++) {
			start = i * (double)N / my_nr_workers;
			end = min((i + 1) * (double)N / my_nr_workers, N);
			dim = end - start;
			MPI_Send(&i,1, MPI_INT, my_workers[i], rand() % 10, MPI_COMM_WORLD);
			MPI_Send(&dim,1, MPI_INT, my_workers[i], rand() % 10, MPI_COMM_WORLD);
			MPI_Send(V + start, dim , MPI_INT, my_workers[i], rand() % 10, MPI_COMM_WORLD);
			
		}

		int recv_mess = 0;
		int id_recv = 0, dim_recv = 0;

		// Astept de la orice worker
		while (recv_mess < my_nr_workers) {
			MPI_Recv(&id_recv, 1,MPI_INT, MPI_ANY_SOURCE,  MPI_ANY_TAG,MPI_COMM_WORLD, &status);
			printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
			MPI_Recv(&dim_recv, 1,MPI_INT, status.MPI_SOURCE,  MPI_ANY_TAG,MPI_COMM_WORLD, NULL);
			printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
			start = id_recv * (double)N / my_nr_workers;
			MPI_Recv(V + start, dim_recv,MPI_INT, status.MPI_SOURCE,  MPI_ANY_TAG,MPI_COMM_WORLD, NULL);
			printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
			recv_mess++;
			
		}
		
	
		// Trimit lui 0 ID-ul bucatii si apoi dimensiunea si datele

		MPI_Send(&vec_id, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
		MPI_Send(&N, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
		MPI_Send(V, N, MPI_INT, 0, 0, MPI_COMM_WORLD);
		MPI_Barrier(MPI_COMM_WORLD); // pentru sincronizare pe stdout
		free(my_workers);
	}

	if (!isCoordonator(rank)) {
		// Primesc in worker  ID-ul si apoi dimensiunea si datele pe care trebuie sa le dublez
		MPI_Recv(&vec_id, 1,MPI_INT, coordonator,  MPI_ANY_TAG,MPI_COMM_WORLD, &status);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		MPI_Recv(&N, 1,MPI_INT, coordonator,  MPI_ANY_TAG,MPI_COMM_WORLD, &status);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		V = malloc(sizeof(int) * N);
		MPI_Recv(V, N,MPI_INT, coordonator,  MPI_ANY_TAG,MPI_COMM_WORLD, &status);
		printf("M(%d,%d)\n", status.MPI_SOURCE, rank);
		for (int i = 0 ; i < N; i++) {
			V[i] *= 2;
		}

		// Trimit coordonatorului corespunzator ID-ul si apoi dimensiunea si datele
		MPI_Send(&vec_id, 1, MPI_INT, coordonator, rand() % 10, MPI_COMM_WORLD);
		MPI_Send(&N, 1, MPI_INT, coordonator, rand() % 10, MPI_COMM_WORLD);
		MPI_Send(V, N, MPI_INT, coordonator, rand() % 10, MPI_COMM_WORLD);
		MPI_Barrier(MPI_COMM_WORLD); // pentru sincronizare pe stdout
		free(V);

	}

	free(V);
	for (int i = 0; i < 3; i++) {
		free(topology[i]);
	}
	free(topology);

	MPI_Finalize();
	return 0;
}