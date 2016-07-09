#include <mpi.h>
#include <mpi-ext.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#define TOPO_DEBUG  0
#define TOPO_HYBRID 0

#ifdef __sparc__
#define TOPO_FJMPICALL(expr) \
    do { \
        int r = (expr); \
        if (r != 0) { \
            fprintf(stderr, "%05d: `" #expr "' is failed with error %d\n",\
                    topo_rank(), r); \
            MPI_Abort(MPI_COMM_WORLD, 1); \
        } \
    } while (0)
#else
#warning "this architecture is not supported"
#define TOPO_FJMPICALL(expr)
#endif

static int topo_rank(void)
{
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return rank;
}

static int topo_node_rank(int rank)
{
    return rank / 16 * 16;
}

static int topo_n_procs(void)
{
    int n_procs;
    MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
    return n_procs;
}

static int topo_dim(void)
{
    int dim;
    TOPO_FJMPICALL(FJMPI_Topology_get_dimension(&dim));
    
    return dim;
}

static void topo_shape(int xyz[3])
{
    TOPO_FJMPICALL(FJMPI_Topology_get_shape(&xyz[0], &xyz[1], &xyz[2]));
}

static void topo_rank2xyz(int rank, int xyz[3])
{
#if TOPO_DEBUG
    fprintf(stderr, "%s(%d, %p):\n", __FUNCTION__, rank, xyz);

    int i;
    for (i = 0; i < 3; i++) xyz[i] = -1;

    assert(rank >= 0);
    assert(rank <= topo_n_procs());
#endif

    int dim = topo_dim();
    int node = topo_node_rank(rank);

    if (dim == 1) {
        TOPO_FJMPICALL(FJMPI_Topology_rank2x(node, &xyz[0]));
    } else if (dim == 2) {
        TOPO_FJMPICALL(FJMPI_Topology_rank2xy(node, &xyz[0], &xyz[1]));
    } else if (dim == 3) {
        TOPO_FJMPICALL(FJMPI_Topology_rank2xyz(node, 
                                               &xyz[0], &xyz[1], &xyz[2]));
    }
}

#if !TOPO_HYBRID
static int topo_xyz2rank(int xyz[3])
{
    int i;

    MPI_Comm comm = MPI_COMM_WORLD;

    int n_procs = topo_n_procs();
    int *gathered = (int *)malloc(sizeof(xyz) * (size_t)n_procs);

    MPI_Allgather(xyz, 3, MPI_INT, gathered, 3, MPI_INT, comm);

    int rank = -1;
    for (i = 0; i < n_procs; i++) {
        int *v = &gathered[3 * i];

        if (v[0] == xyz[0] && v[1] == xyz[1] && v[2] == xyz[2]) {
            rank = topo_node_rank(i);
            break;
        }
    }

    free(gathered);

    return rank;
}
#else
static int topo_xyz2rank(int xyz[3])
{
    int i;

#if TOPO_DEBUG
    fprintf(stderr, "%s(xyz=(%d, %d, %d)):\n", 
            __FUNCTION__, xyz[0], xyz[1], xyz[2]);
#endif

    MPI_Comm comm = MPI_COMM_WORLD;
    int tag = 0;

    int me = topo_rank();
    int n_procs = topo_n_procs();

    if (n_procs <= 1)  // FJMPI returns error for unknown reason
        return 0;

    int dim = topo_dim();

    int rank;
    if (dim == 1) {
        TOPO_FJMPICALL(FJMPI_Topology_x2rank(xyz[0], &rank));
    } else if (dim == 2) {
        TOPO_FJMPICALL(FJMPI_Topology_xy2rank(xyz[0], xyz[1], &rank));
    } else if (dim == 3) {
        TOPO_FJMPICALL(
            FJMPI_Topology_xyz2rank(xyz[0], xyz[1], xyz[2], &rank));
    }

    return rank;
}
#endif

static int mod(int rank, int n_ranks)
{
    return (rank + n_ranks) % n_ranks;
}

#if !TOPO_HYBRID
static void topo_neighbs(int rank, int neighbs[6])
{
    int i;

    MPI_Comm comm = MPI_COMM_WORLD;

    int n_procs = topo_n_procs();

    int dim = topo_dim();

    int shape[3];
    topo_shape(shape);

    int xyz[3];
    topo_rank2xyz(rank, xyz);

    int *gathered = (int *)malloc(sizeof(xyz) * (size_t)n_procs);
    MPI_Allgather(xyz, 3, MPI_INT, gathered, 3, MPI_INT, comm);

    for (i = 0; i < 6; i++)
        neighbs[i] = 0;

    int xyz_1[] = { (dim >= 1) ? mod(xyz[0] - 1, shape[0]) : 0,
                    (dim >= 2) ? mod(xyz[1] - 1, shape[1]) : 0,
                    (dim >= 3) ? mod(xyz[2] - 1, shape[2]) : 0 };
    int xyz_2[] = { (dim >= 1) ? mod(xyz[0] + 1, shape[0]) : 0,
                    (dim >= 2) ? mod(xyz[1] + 1, shape[1]) : 0,
                    (dim >= 3) ? mod(xyz[2] + 1, shape[2]) : 0 };

    for (i = 0; i < n_procs; i++) {
        int *v = &gathered[3 * i];

        if (dim >= 1) {
            if (v[0] == xyz_1[0] && v[1] == xyz[1] && v[2] == xyz[2])
                neighbs[0] = i;
            if (v[0] == xyz_2[0] && v[1] == xyz[1] && v[2] == xyz[2])
                neighbs[1] = i;
        }
        if (dim >= 2) {
            if (v[0] == xyz[0] && v[1] == xyz_1[1] && v[2] == xyz[2])
                neighbs[2] = i;
            if (v[0] == xyz[0] && v[1] == xyz_2[1] && v[2] == xyz[2])
                neighbs[3] = i;
        }
        if (dim >= 3) {
            if (v[0] == xyz[0] && v[1] == xyz[1] && v[2] == xyz_1[2])
                neighbs[4] = i;
            if (v[0] == xyz[0] && v[1] == xyz[1] && v[2] == xyz_2[2])
                neighbs[5] = i;
        }
    }

    free(gathered);

    for (i = 0; i < 6; i++)
        neighbs[i] = topo_node_rank(neighbs[i]);
}
#else
static void topo_neighbs(int rank, int neighbs[6])
{
#if TOPO_DEBUG
    assert(rank >= 0);
    assert(rank < topo_n_procs());

    int i;
    for (i = 0; i < 6; i++)
        neighbs[i] = -1;
#endif

    int dim = topo_dim();

    int shape[3];
    topo_shape(shape);

    int rank3d[3];
    topo_rank2xyz(rank, rank3d);

    int xyz0[] = { mod(rank3d[0] - 1, shape[0]), rank3d[1], rank3d[2] };
    neighbs[0] = topo_xyz2rank(xyz0);

    int xyz1[] = { mod(rank3d[0] + 1, shape[0]), rank3d[1], rank3d[2] };
    neighbs[1] = topo_xyz2rank(xyz1);

    if (dim >= 2) {
        int xyz2[] = { rank3d[0], mod(rank3d[1] - 1, shape[1]), rank3d[2] };
        neighbs[2] = topo_xyz2rank(xyz2);

        int xyz3[] = { rank3d[0], mod(rank3d[1] + 1, shape[1]), rank3d[2] };
        neighbs[3] = topo_xyz2rank(xyz3);
    }

    if (dim >= 3) {
        int xyz4[] = { rank3d[0], rank3d[1], mod(rank3d[2] - 1, shape[2]) };
        neighbs[4] = topo_xyz2rank(xyz4);

        int xyz5[] = { rank3d[0], rank3d[1], mod(rank3d[2] + 1, shape[2]) };
        neighbs[5] = topo_xyz2rank(xyz5);
    }
}
#endif

static int topo_random_int(int n)
{
    assert(n > 0);

    if (n == 1)
        return 0;

    size_t rand_max =
        ((size_t)RAND_MAX + 1) - ((size_t)RAND_MAX + 1) % (size_t)n;
    int r;
    do {
       r = rand();
    } while ((size_t)r >= rand_max);

    return (int)((double)n * (double)r / (double)rand_max);
}

static int topo_rand_neighb(int neighbs[6], int rank)
{
    int dim = topo_dim();

    int target;
    do {
        int target_node = topo_random_int(2 * dim + 1);
        int target_core = topo_random_int(16);

        if (target_node == 2 * dim) {
            int node = topo_node_rank(rank);
            target = node + target_core;
        } else {
            target = neighbs[target_node] + target_core;
        }
    } while (target == rank);

    return target;
} 

