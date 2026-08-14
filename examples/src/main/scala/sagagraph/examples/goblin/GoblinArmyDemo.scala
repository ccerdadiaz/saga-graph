package sagagraph.examples.goblin

import sagagraph.*
import sagagraph.store.sqlite.SqliteWalStore

// ---------------------------------------------------------------------------
// GoblinArmyDemo — arms 5 goblins with limited resources
//
// Stock: 3 weapons, 4 uniforms, 2 boots
// Expected: some goblins fully armed, some partially, some compensated
// ---------------------------------------------------------------------------
object GoblinArmyDemo extends App:

  val goblins = List("Grishnakh", "Ugluk", "Muzgash", "Lagduf", "Snaga")

  println("=== DARK LORD'S ARMY RECRUITMENT — Operation: Ready for Battle ===")
  println(s"Recruits: ${goblins.mkString(", ")}")
  println(s"Stock: 3 weapons | 4 uniforms | 2 boots")
  println("=" * 60)

  val results = goblins.map { name =>
    val store = SqliteWalStore(s"examples/target/$name.db")
    println(s"\n>> Arming $name...")
    val result = ArmGoblinSaga(name, store)
    name -> result
  }

  println("\n" + "=" * 60)
  println("=== RECRUITMENT REPORT ===")
  results.foreach { case (name, result) =>
    val status = result match
      case SagaResult.Completed => "✓ ARMED AND READY"
      case SagaResult.Failed(e) => s"✗ FAILED — ${e.getMessage}"
    println(s"  $name: $status")
  }
  println("=" * 60)
  println("The Dark Lord will be... partially satisfied.")
