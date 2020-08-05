using System;
using System.Threading.Tasks;
using DotNetCoreSqlDb.Data;
using Microsoft.AspNetCore.Mvc;
using DotNetCoreSqlDb.Models;

namespace DotNetCoreSqlDb.Controllers
{
    public class TodosController : Controller
    {
        private ITodoRepository _Repository;

        public TodosController( ITodoRepository repository )
        {
            _Repository = repository;
        }

        // GET: Todos
        public async Task<IActionResult> Index()
        {
            return View( await _Repository.GetAllAsync(  ) );
        }

        // GET: Todos/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var todo = await _Repository.GetByIdAsync( id.Value );
            if (todo == null)
            {
                return NotFound();
            }

            return View(todo);
        }

        // GET: Todos/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: Todos/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("ID,Description,CreatedDate")] Todo todo)
        {
            if (ModelState.IsValid)
            {
                await _Repository.StoreAsync( todo );
                return RedirectToAction(nameof(Index));
            }
            return View(todo);
        }

        // GET: Todos/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var todo = await _Repository.GetByIdAsync( id.Value );
            if (todo == null)
            {
                return NotFound();
            }
            return View(todo);
        }

        // POST: Todos/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("ID,Description,CreatedDate,VersionString")] Todo todo)
        {
            if (id != todo.ID)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                var count = await _Repository.StoreAsync( todo );
                if ( count != 1 )
                {
                    return NotFound();
                }
                return RedirectToAction(nameof(Index));
            }
            return View(todo);
        }

        // GET: Todos/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var todo = await _Repository.GetByIdAsync( id.Value );
            if (todo == null)
            {
                return NotFound();
            }

            return View(todo);
        }

        // POST: Todos/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id, string versionString)
        {
            var version = Convert.FromBase64String( versionString );
            var count = await _Repository.DeleteAsync( id, version );
            if ( count != 1 )
            {
                return NotFound();
            }
            return RedirectToAction(nameof(Index));
        }
    }
}
