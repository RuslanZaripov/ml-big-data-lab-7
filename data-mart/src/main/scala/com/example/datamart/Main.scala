package com.example.datamart

import cats.effect.{IO, IOApp}

object Main extends IOApp.Simple {
  val run = DatamartServer.run[IO]
}
