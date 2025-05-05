package com.example.datamart

import cats.effect.Sync
import cats.implicits._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

object DatamartRoutes {

  def jokeRoutes[F[_]: Sync](J: Jokes[F]): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._
    HttpRoutes.of[F] { case GET -> Root / "joke" =>
      for {
        joke <- J.get
        resp <- Ok(joke)
      } yield resp
    }
  }

  def helloWorldRoutes[F[_]: Sync](H: HelloWorld[F]): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._
    HttpRoutes.of[F] { case GET -> Root / "hello" / name =>
      for {
        greeting <- H.hello(HelloWorld.Name(name))
        resp <- Ok(greeting)
      } yield resp
    }
  }

  def dataProcessorRoutes[F[_]: Sync](D: DataProcessor[F]): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    HttpRoutes.of[F] {
      case GET -> Root / "process-data" / IntVar(numPartitions)
          if numPartitions >= 1 && numPartitions <= 100 =>
        for {
          result <- D.processData(DataProcessor.NumPartitions(numPartitions))
          resp <- Ok(result)
        } yield resp

      case GET -> Root / "process-data" / _ =>
        BadRequest("Number of partitions must be between 1 and 100")
    }
  }
}
