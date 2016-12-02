using System.Collections.Generic;

namespace ParallelTaskRunner
{
    public class TasksState<T>
    {
        public List<T> TasksToDo { get; set; }
        public HashSet<T> DoneTasks { get; set; }
        public HashSet<T> TasksInProgress { get; set; }
    }
}
