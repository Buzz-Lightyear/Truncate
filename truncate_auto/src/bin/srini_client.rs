use tokio::sync::mpsc;
use tonic::Request;
use truncate_auto::player_move_to_move;
use truncate_auto::service::{self, move_request, MoveRequest, PlaceMove, PlayGameReply, SwapMove};

use service::play_game_request;
use service::truncate_client::TruncateClient;
use service::{InitRequest, PlayGameRequest};
use truncate_core::board::{Board, Coordinate, Square, SquareValidity};
use truncate_core::game::Game;
use truncate_core::moves::Move;
use truncate_core::npc::scoring::NPCPersonality;
use truncate_core::player::Hand;
use truncate_core::rules::GameRules;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dict = truncate_auto::init_dict()?;
    let mut client = TruncateClient::connect("http://[::1]:50051").await.unwrap();

    let (tx, mut rx) = mpsc::channel(128);

    let mut game = Game::new(9, 9, None, GameRules::generation(2));

    let outbound = async_stream::stream! {
        let mut req_count = 0;
        let mut request_id = || -> String {
            req_count += 1;
            req_count.to_string()
        };

        // First request
        let name = "Srini".to_string();
        println!("Sending initial PlayGameRequest with player name: {}", name);
        yield PlayGameRequest{
            request_id: request_id(),
            request: Some(play_game_request::Request::InitRequest(InitRequest{
                player_name: name,
            })),
        };

        // This should be the play response
        let play_resp: PlayGameReply = match rx.recv().await {
            Some(pgr) => pgr,
            None => {
                eprintln!("channel closed before first response");
                return;
            },
        };
        let init_reply = match play_resp.reply {
            Some(service::play_game_reply::Reply::InitReply(ir)) => ir,
            Some(_) => {
                eprintln!("initial response was not a PlayReply");
                return;
            },
            None => {
                eprintln!("initial response had no actual response");
                return;
            },
        };

        let player_id = init_reply.player_id;
        let mut opp_index = 0;
        for i in 0..(init_reply.opponents.len() + 1) {
            if i == player_id as usize {
                game.add_player("Test Bot!".to_string());
            } else {
                game.add_player(init_reply.opponents[opp_index].name.to_string());
                opp_index += 1;
            }
        }
        game.players.get_mut(player_id as usize).unwrap().hand = to_hand(&init_reply.hand);
        game.rules.battle_delay = 0;
        game.start();

        loop {
            // Wait til we're told to move
            let wire_board = loop {
                let resp = match rx.recv().await {
                    Some(resp) => resp,
                    None => {

                                eprintln!("channel closed before expected");
                                return;
                    },
                };
                let ms = match resp.reply {
                    Some(service::play_game_reply::Reply::MoveSolicitation(ms)) => ms,
                    Some(service::play_game_reply::Reply::PlayerMove(pm)) => match player_move_to_move(&pm) {
                        Some(mv) => {
                            // Add the move to our board.
                            println!("Adding move {:?} from player {} to our board", mv, pm.player_id);

                            // So we don't know the other player's hand, but the
                            // game expects to have all that information, so we
                            // just set their hand to the value they just played
                            // to placate the game.
                            if let Move::Place{player, tile, ..} = mv {
                                game.players.get_mut(player).unwrap().hand = Hand(vec![tile]);
                            }
                            game.play_turn(mv, Some(&dict), Some(&dict), None).unwrap();

                            continue;
                        },
                        None => {
                            eprintln!("no move in player move from {}", pm.player_id);
                            return;
                        }
                    },
                    Some(v) => {
                        println!("ignoring message {:?} that isn't a move solicitation", v);
                        continue;
                    },
                    _ => {
                        eprintln!("response had no actual response");
                        return;
                    }
                };
                break match ms.board{
                    Some(b) => b,
                    None => {
                        eprintln!("initial response had no board");
                        return;
                    },
                };
            };
            game.board = to_board(&wire_board);
            game.board.cache_special_squares();

            let mut arb = truncate_core::npc::Arborist::pruning();
            let npc = NPCPersonality::jet();
            arb.capped(npc.params.evaluation_cap);

            println!("Got board!\n\n{}\n\nPLayer ID {}", game.board, player_id);
            let (player_msg, _board_score) = Game::srini_move(
                &game,
                Some(&dict),
                Some(&dict),
                npc.params.max_depth,
                Some(&mut arb),
                false,
                &npc.params,
            );
            let move_msg = match player_msg {
                truncate_core::messages::PlayerMessage::Place(coor, c) => {
                    println!("placing {:?} at {:?}", c, coor);
                    game.play_turn(truncate_core::moves::Move::Place {
                        player: player_id as usize,
                        tile: c,
                        position: Coordinate {
                            x: coor.x as usize,
                            y: coor.y as usize,
                        }
                    }, Some(&dict), Some(&dict), None).unwrap();
                    PlayGameRequest{
                        request_id: request_id(),
                        request: Some(play_game_request::Request::MoveRequest(MoveRequest{
                            r#move: Some(move_request::Move::PlaceMove(PlaceMove{
                                tile: c.to_string(),
                                position: to_coord(&coor),
                            }))
                        })),
                    }
                },
                truncate_core::messages::PlayerMessage::Swap(from, to) => {
                    println!("swapping tile at {:?} and {:?}", from, to);
                    game.play_turn(truncate_core::moves::Move::Swap {
                        player: player_id as usize,
                        positions: [Coordinate {
                            x: from.x as usize,
                            y: from.y as usize,
                        },Coordinate {
                            x: to.x as usize,
                            y: to.y as usize,
                        }]
                    }, Some(&dict), Some(&dict), None).unwrap();
                    PlayGameRequest{
                        request_id: request_id(),
                        request: Some(play_game_request::Request::MoveRequest(MoveRequest{
                            r#move: Some(move_request::Move::SwapMove(SwapMove{
                                from: to_coord(&from),
                                to: to_coord(&to),
                            }))
                        })),
                    }
                },
                v => {
                    eprintln!("unexpected player message {:?}", v);
                    break
                },
            };

            yield move_msg;


            let mr = match rx.recv().await {
                Some(PlayGameReply { reply: Some(service::play_game_reply::Reply::MoveReply(mr)), .. }) => mr,
                Some(v) => {
                    eprintln!("response was not a MoveReply {:?}", v);
                    return;
                }
                None => {
                    eprintln!("channel closed while reading move response");
                    return;
                },
            };
            game.players.get_mut(player_id as usize).unwrap().hand = to_hand(&mr.hand);
            if mr.game_over {
                println!("game is over!");
                return;
            }
         }
    };

    let response = client.play_game(Request::new(outbound)).await?;
    let mut inbound = response.into_inner();

    while let Some(msg) = inbound.message().await? {
        tx.send(msg).await?;
    }

    Ok(())
}

fn to_coord(c: &Coordinate) -> Option<service::Coordinate> {
    Some(service::Coordinate {
        x: c.x as u32,
        y: c.y as u32,
    })
}

fn square_validity(v: i32) -> SquareValidity {
    match v {
        0 => SquareValidity::Unknown,
        1 => SquareValidity::Valid,
        2 => SquareValidity::Invalid,
        3 => SquareValidity::Partial,
        _ => panic!("invalid square_validity value"),
    }
}

fn board_tile_to_square(sq: &service::Tile) -> Square {
    match &sq.tile {
        Some(t) => match t {
            service::tile::Tile::Water(t) => Square::Water { foggy: t.foggy },
            service::tile::Tile::Land(t) => Square::Land { foggy: t.foggy },
            service::tile::Tile::Town(t) => Square::Town {
                player: t.player as usize,
                defeated: t.defeated,
                foggy: t.foggy,
            },
            service::tile::Tile::Obelisk(t) => Square::Obelisk { foggy: t.foggy },
            service::tile::Tile::Artifact(t) => Square::Artifact {
                player: t.player as usize,
                defeated: t.defeated,
                foggy: t.foggy,
            },
            service::tile::Tile::Occupied(t) => Square::Occupied {
                player: t.player as usize,
                tile: t.tile.chars().next().unwrap(),
                validity: square_validity(t.validity),
                foggy: t.foggy,
            },
            service::tile::Tile::Fog(_t) => Square::Fog {},
        },
        None => todo!(),
    }
}

fn to_squares(sqs: &service::Squares) -> Vec<Square> {
    sqs.tiles.iter().map(|s| board_tile_to_square(&s)).collect()
}

fn to_board(b: &service::Board) -> Board {
    let mut board = Board::new(9, 9);

    let mut squares: Vec<Vec<Square>> = vec![];

    for row in &b.squares {
        squares.push(to_squares(row))
    }

    board.squares = squares;
    board.cache_special_squares();

    board
}

fn to_hand(h: &Vec<String>) -> Hand {
    Hand(h.iter().map(|v| v.chars().next().unwrap()).collect())
}
